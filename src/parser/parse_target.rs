/*-------------------------------------------------------------------------
 *
 * parse_target.rs
 *   handle target lists
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *   src/backend/parser/parse_target.c
 *
 *-------------------------------------------------------------------------
 */

#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(unused_mut)]
#![allow(unused_imports)]
#![allow(unreachable_patterns)]

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void};
use core::mem::size_of;

use crate::{castNode, current_cell, foreach, intVal, lfirst_node, linitial_node, list_make1, list_make2, makeNode, strVal, IsA};

use crate::postgres_ext::Oid;
use crate::postgres::Datum;
use crate::c::{OidIsValid, int32};

use crate::nodes::nodes::{nodeTag, Node, NodeTag, NodeTag::*};
use crate::nodes::pg_list::{
    List, NIL,
    lfirst, lfirst_int, lfirst_oid, linitial, lsecond, lthird, llast, lnext,
    lappend, lappend_int, lappend_oid, lcons, list_head,
    list_concat, list_length, list_make1_impl,
    list_nth, list_truncate, list_member_int,
    list_copy, ListCell,
};
use crate::nodes::bitmapset::{Bitmapset, bms_add_member, bms_is_member};
use crate::nodes::nodeFuncs::{
    exprType, exprTypmod, exprCollation, exprLocation,
    strip_implicit_coercions,
};
use crate::nodes::makefuncs::{
    makeConst, makeBoolExpr, makeVar, makeTargetEntry,
    makeSimpleA_Expr, makeNullConst, makeRangeVar,
    RECORDOID,
};
use crate::nodes::value::{makeString, String as PgString};
use crate::nodes::primnodes::{
    Expr, Var, Alias, TargetEntry, JoinExpr, RangeTblRef, RangeVar,
    CoalesceExpr, BoolExpr, BoolExprType,
    CoercionForm, CoercionForm::*,
    CoercionContext, CoercionContext::*,
    FieldSelect, FieldStore, SubscriptingRef,
    CaseTestExpr, SubLink, SubLinkType,
    SQLValueFunction, SQLValueFunctionOp,
    XmlExpr, XmlExprOp,
    Param, PARAM_EXEC,
    CaseExpr, SetToDefault,
    MinMaxExpr, MinMaxOp,
    GroupingFunc, MergeSupportFunc,
    JsonExprOp,
};
use crate::nodes::parsenodes::{
    Query, ColumnRef, A_Const, A_Expr, A_Expr_Kind as A_ExprKind, FuncCall, SortBy,
    SelectStmt, ResTarget,
    RangeTblEntry, RTEPermissionInfo, CommonTableExpr,
    RTEKind, RTEKind::*,
    A_Indirection, A_Star, A_Indices, TypeCast, CollateClause, TypeName,
    XmlSerialize,
    JsonParseExpr, JsonScalarExpr, JsonSerializeExpr, JsonFuncExpr,
    JsonObjectConstructor, JsonArrayConstructor, JsonArrayQueryConstructor,
    JsonObjectAgg, JsonArrayAgg,
};

use crate::parser::parse_node::{
    cancel_parser_errposition_callback, parser_errposition,
    setup_parser_errposition_callback,
    Index, ParseCallbackState, ParseExprKind, ParseExprKind::*,
    ParseNamespaceColumn, ParseNamespaceItem, ParseState,
    transformContainerType, transformContainerSubscripts, Relation,
};
use crate::parser::parse_expr::transformExpr;
use crate::parser::parse_coerce::{
    coerce_type, coerce_to_target_type, coerce_to_domain,
};
use crate::parser::parse_relation::{
    refnameNamespaceItem, GetNSItemByRangeTablePosn, GetRTEByRangeTablePosn,
    GetCTEForRTE,
    expandNSItemVars, expandNSItemAttrs,
    markVarForSelectPriv,
    errorMissingRTE,
    get_tle_by_resno, attnumTypeId, attnameAttNum,
    get_rte_attribute_name,
};
use crate::nodes::parsenodes::GetCTETargetList;

use crate::access::common::tupdesc::{
    TupleDesc, CreateTemplateTupleDesc, TupleDescInitEntry, TupleDescInitEntryCollation,
};
use crate::access::htup_details::{HeapTuple};
use crate::access::attnum::{AttrNumber, InvalidAttrNumber};

use crate::utils::cache::lsyscache::{
    format_type_be, get_typcollation,
    get_attnum, get_atttypetypmodcoll,
    pstrdup, palloc, pfree,
};
use crate::parser::parse_type::typeidTypeRelid;
use crate::utils::cache::lsyscache::getBaseTypeAndTypmod;

use crate::catalog::pg_type_d::{BYTEAOID};
use crate::postgres_ext::InvalidOid;
use crate::c::{NameStr, NameData};

// ---------------------------------------------------------------------------
// Stub for expandRTE (used in expandRecordVariable)
// ---------------------------------------------------------------------------
use crate::parser::parse_relation::expandRTE;

// ---------------------------------------------------------------------------
// Stubs for unported dependencies
// ---------------------------------------------------------------------------

// TODO(pg-port): commands/dbcommands.c get_database_name
unsafe fn get_database_name(_dbid: Oid) -> *mut c_char {
    b"template1\0".as_ptr() as *mut c_char
}

// TODO(pg-port): miscadmin.h MyDatabaseId
static mut MyDatabaseId: Oid = 0;

// TODO(pg-port): nodes/nodeFuncs.c get_expr_result_tupdesc
unsafe fn get_expr_result_tupdesc(expr: *mut Node, noerror: bool) -> TupleDesc {
    core::ptr::null_mut()
}

// TODO(pg-port): catalog/namespace.c NameListToString
unsafe fn NameListToString(names: *mut List) -> *mut c_char {
    b"<namelist>\0".as_ptr() as *mut c_char
}

// TODO(pg-port): nodes/parsenodes.h TupleDescAttr macro
unsafe fn TupleDescAttr(tupdesc: TupleDesc, attnum: c_int) -> *mut FormData_pg_attribute {
    core::ptr::null_mut()
}

// Form_pg_attribute stub
struct FormData_pg_attribute {
    attisdropped: bool,
    atttypid: Oid,
    atttypmod: int32,
    attcollation: Oid,
    attname: NameData,
}
type Form_pg_attribute = *mut FormData_pg_attribute;

// TODO(pg-port): access/rel.h ACL_SELECT
type AclMode = u64;
const ACL_SELECT: AclMode = 0x0002;

// libc strcmp wrapper
unsafe fn libc_strcmp(a: *const c_char, b: *const c_char) -> c_int {
    let sa = std::ffi::CStr::from_ptr(a);
    let sb = std::ffi::CStr::from_ptr(b);
    match sa.cmp(sb) {
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => 0,
        std::cmp::Ordering::Greater => 1,
    }
}

/*
 * transformTargetEntry()
 *	Transform any ordinary "expression-type" node into a targetlist entry.
 */
pub unsafe fn transformTargetEntry(
    pstate: *mut ParseState,
    node: *mut Node,
    mut expr: *mut Node,
    exprKind: ParseExprKind,
    colname: *mut c_char,
    resjunk: bool,
) -> *mut TargetEntry {
    /* Transform the node if caller didn't do it already */
    if expr.is_null() {
        /*
         * If it's a SetToDefault node and we should allow that, pass it
         * through unmodified.
         */
        if exprKind == EXPR_KIND_UPDATE_SOURCE && IsA!(node, T_SetToDefault) {
            expr = node;
        } else {
            expr = transformExpr(pstate, node, exprKind);
        }
    }

    let mut colname = colname;
    if colname.is_null() && !resjunk {
        /*
         * Generate a suitable column name for a column without any explicit
         * 'AS ColumnName' clause.
         */
        colname = FigureColname(node);
    }

    makeTargetEntry(
        expr as *mut Expr,
        (*pstate).p_next_resno as i16,
        colname,
        resjunk,
    )
}

/*
 * transformTargetList()
 * Turns a list of ResTarget's into a list of TargetEntry's.
 */
pub unsafe fn transformTargetList(
    pstate: *mut ParseState,
    targetlist: *mut List,
    exprKind: ParseExprKind,
) -> *mut List {
    let mut p_target: *mut List = NIL;
    let expand_star: bool;
    let mut o_target: *mut ListCell;

    /* Shouldn't have any leftover multiassign items at start */
    assert!((*pstate).p_multiassign_exprs.is_null());

    /* Expand "something.*" in SELECT and RETURNING, but not UPDATE */
    expand_star = exprKind != EXPR_KIND_UPDATE_SOURCE;

    foreach!(o_target, targetlist, {
        let res: *mut ResTarget = lfirst(current_cell!(o_target)) as *mut ResTarget;

        /*
         * Check for "something.*".
         */
        if expand_star {
            if IsA!((*res).val, T_ColumnRef) {
                let cref: *mut ColumnRef = (*res).val as *mut ColumnRef;

                if IsA!(llast((*cref).fields), T_A_Star) {
                    /* It is something.*, expand into multiple items */
                    p_target = list_concat(
                        p_target,
                        ExpandColumnRefStar(pstate, cref, true),
                    );
                    continue;
                }
            } else if IsA!((*res).val, T_A_Indirection) {
                let ind: *mut A_Indirection = (*res).val as *mut A_Indirection;

                if IsA!(llast((*ind).indirection), T_A_Star) {
                    /* It is something.*, expand into multiple items */
                    p_target = list_concat(
                        p_target,
                        ExpandIndirectionStar(pstate, ind, true, exprKind),
                    );
                    continue;
                }
            }
        }

        /*
         * Not "something.*", so transform as a single expression
         */
        p_target = lappend(
            p_target,
            transformTargetEntry(
                pstate,
                (*res).val,
                core::ptr::null_mut(),
                exprKind,
                (*res).name,
                false,
            ) as *mut c_void,
        );
    });

    /*
     * If any multiassign resjunk items were created, attach them to the end
     * of the targetlist.
     */
    if !(*pstate).p_multiassign_exprs.is_null() {
        assert!(exprKind == EXPR_KIND_UPDATE_SOURCE);
        p_target = list_concat(p_target, (*pstate).p_multiassign_exprs);
        (*pstate).p_multiassign_exprs = NIL;
    }

    p_target
}

/*
 * transformExpressionList()
 *
 * This is the identical transformation to transformTargetList, except that
 * the input list elements are bare expressions without ResTarget decoration.
 */
pub unsafe fn transformExpressionList(
    pstate: *mut ParseState,
    exprlist: *mut List,
    exprKind: ParseExprKind,
    allowDefault: bool,
) -> *mut List {
    let mut result: *mut List = NIL;
    let mut lc: *mut ListCell;

    foreach!(lc, exprlist, {
        let mut e: *mut Node = lfirst(current_cell!(lc)) as *mut Node;

        /*
         * Check for "something.*".
         */
        if IsA!(e, T_ColumnRef) {
            let cref: *mut ColumnRef = e as *mut ColumnRef;

            if IsA!(llast((*cref).fields), T_A_Star) {
                /* It is something.*, expand into multiple items */
                result = list_concat(result, ExpandColumnRefStar(pstate, cref, false));
                continue;
            }
        } else if IsA!(e, T_A_Indirection) {
            let ind: *mut A_Indirection = e as *mut A_Indirection;

            if IsA!(llast((*ind).indirection), T_A_Star) {
                /* It is something.*, expand into multiple items */
                result = list_concat(
                    result,
                    ExpandIndirectionStar(pstate, ind, false, exprKind),
                );
                continue;
            }
        }

        /*
         * Not "something.*", so transform as a single expression.
         */
        if allowDefault && IsA!(e, T_SetToDefault) {
            /* do nothing */
        } else {
            e = transformExpr(pstate, e, exprKind);
        }

        result = lappend(result, e as *mut c_void);
    });

    result
}

/*
 * resolveTargetListUnknowns()
 *		Convert any unknown-type targetlist entries to type TEXT.
 */
pub unsafe fn resolveTargetListUnknowns(pstate: *mut ParseState, targetlist: *mut List) {
    let mut l: *mut ListCell;
    const TEXTOID: Oid = 25;
    const UNKNOWNOID: Oid = 705;

    foreach!(l, targetlist, {
        let tle: *mut TargetEntry = lfirst(current_cell!(l)) as *mut TargetEntry;
        let restype: Oid = exprType((*tle).expr as *mut Node);

        if restype == UNKNOWNOID {
            (*tle).expr = coerce_type(
                pstate,
                (*tle).expr as *mut Node,
                restype,
                TEXTOID,
                -1,
                COERCION_IMPLICIT,
                COERCE_IMPLICIT_CAST,
                -1,
            ) as *mut Expr;
        }
    });
}

/*
 * markTargetListOrigins()
 *		Mark targetlist columns that are simple Vars with the source
 *		table's OID and column number.
 */
pub unsafe fn markTargetListOrigins(pstate: *mut ParseState, targetlist: *mut List) {
    let mut l: *mut ListCell;

    foreach!(l, targetlist, {
        let tle: *mut TargetEntry = lfirst(current_cell!(l)) as *mut TargetEntry;
        markTargetListOrigin(pstate, tle, (*tle).expr as *mut Var, 0);
    });
}

/*
 * markTargetListOrigin()
 *		If 'var' is a Var of a plain relation, mark 'tle' with its origin
 */
unsafe fn markTargetListOrigin(
    pstate: *mut ParseState,
    tle: *mut TargetEntry,
    var: *mut Var,
    levelsup: c_int,
) {
    let netlevelsup: c_int;
    let rte: *mut RangeTblEntry;
    let attnum: AttrNumber;

    if var.is_null() || !IsA!(var as *mut Node, T_Var) {
        return;
    }
    netlevelsup = (*var).varlevelsup as c_int + levelsup;
    rte = GetRTEByRangeTablePosn(pstate, (*var).varno, netlevelsup);
    attnum = (*var).varattno;

    match (*rte).rtekind {
        RTE_RELATION => {
            /* It's a table or view, report it */
            (*tle).resorigtbl = (*rte).relid;
            (*tle).resorigcol = attnum;
        }
        RTE_SUBQUERY => {
            /* Subselect-in-FROM: copy up from the subselect */
            if attnum != InvalidAttrNumber {
                let ste: *mut TargetEntry =
                    get_tle_by_resno((*(*rte).subquery).targetList, attnum);

                if ste.is_null() || (*ste).resjunk {
                    panic!(
                        "subquery {} does not have attribute {}",
                        std::ffi::CStr::from_ptr((*(*rte).eref).aliasname).to_string_lossy(),
                        attnum
                    );
                }
                (*tle).resorigtbl = (*ste).resorigtbl;
                (*tle).resorigcol = (*ste).resorigcol;
            }
        }
        RTE_JOIN | RTE_FUNCTION | RTE_VALUES | RTE_TABLEFUNC | RTE_NAMEDTUPLESTORE
        | RTE_RESULT => {
            /* not a simple relation, leave it unmarked */
        }
        RTE_CTE => {
            /*
             * CTE reference: copy up from the subquery, if possible.
             */
            if attnum != InvalidAttrNumber && !(*rte).self_reference {
                let cte: *mut CommonTableExpr =
                    GetCTEForRTE(pstate, rte, netlevelsup);
                let ste: *mut TargetEntry;
                let tl: *mut List = GetCTETargetList(cte);
                let mut extra_cols: c_int = 0;

                /*
                 * RTE for CTE will already have the search and cycle columns
                 * added, but the subquery won't, so skip looking those up.
                 */
                if !(*cte).search_clause.is_null() {
                    extra_cols += 1;
                }
                if !(*cte).cycle_clause.is_null() {
                    extra_cols += 2;
                }
                if extra_cols != 0
                    && attnum > list_length(tl) as AttrNumber
                    && attnum <= (list_length(tl) + extra_cols) as AttrNumber
                {
                    return; /* skip search/cycle columns */
                }

                ste = get_tle_by_resno(tl, attnum);
                if ste.is_null() || (*ste).resjunk {
                    panic!(
                        "CTE {} does not have attribute {}",
                        std::ffi::CStr::from_ptr((*(*rte).eref).aliasname).to_string_lossy(),
                        attnum
                    );
                }
                (*tle).resorigtbl = (*ste).resorigtbl;
                (*tle).resorigcol = (*ste).resorigcol;
            }
        }
        RTE_GROUP => {
            /* We couldn't get here: the RTE_GROUP RTE has not been added */
        }
        _ => {}
    }
}

/*
 * transformAssignedExpr()
 *	This is used in INSERT and UPDATE statements only.
 */
pub unsafe fn transformAssignedExpr(
    pstate: *mut ParseState,
    mut expr: *mut Expr,
    exprKind: ParseExprKind,
    colname: *const c_char,
    attrno: c_int,
    indirection: *mut List,
    location: c_int,
) -> *mut Expr {
    use crate::utils::rel::RelationData;
    let rd: *mut RelationData = (*pstate).p_target_relation as *mut RelationData;
    let type_id: Oid;           // type of value provided
    let attrtype: Oid;          // type of target column
    let attrtypmod: int32;
    let attrcollation: Oid;     // collation of target column
    let sv_expr_kind: ParseExprKind;

    /*
     * Save and restore identity of expression type we're parsing.
     */
    assert!(exprKind != EXPR_KIND_NONE);
    sv_expr_kind = (*pstate).p_expr_kind;
    (*pstate).p_expr_kind = exprKind;

    assert!(!rd.is_null());
    if attrno <= 0 {
        ereport!(ERROR,
            errmsg!("cannot assign to system column \"{}\"",
                std::ffi::CStr::from_ptr(colname).to_string_lossy())
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
               parser_errposition(pstate, location) */
        );
    }
    attrtype = attnumTypeId(rd as Relation, attrno);
    attrtypmod = (*TupleDescAttr((*rd).rd_att, attrno - 1)).atttypmod;
    attrcollation = (*TupleDescAttr((*rd).rd_att, attrno - 1)).attcollation;

    /*
     * If the expression is a DEFAULT placeholder, insert the attribute's
     * type/typmod/collation into it.
     */
    if !expr.is_null() && IsA!(expr as *mut Node, T_SetToDefault) {
        let def: *mut SetToDefault = expr as *mut SetToDefault;

        (*def).typeId = attrtype;
        (*def).typeMod = attrtypmod;
        (*def).collation = attrcollation;
        if !indirection.is_null() {
            if IsA!(linitial(indirection), T_A_Indices) {
                ereport!(ERROR,
                    errmsg!("cannot set an array element to DEFAULT")
                    /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                       parser_errposition(pstate, location) */
                );
            } else {
                ereport!(ERROR,
                    errmsg!("cannot set a subfield to DEFAULT")
                    /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                       parser_errposition(pstate, location) */
                );
            }
        }
    }

    /* Now we can use exprType() safely. */
    type_id = exprType(expr as *mut Node);

    /*
     * If there is indirection on the target column, prepare an array or
     * subfield assignment expression.
     */
    if !indirection.is_null() {
        let colVar: *mut Node;

        if (*pstate).p_is_insert {
            /*
             * The command is INSERT INTO table (col.something) ... so there
             * is not really a source value to work with.
             */
            colVar = makeNullConst(attrtype, attrtypmod, attrcollation) as *mut Node;
        } else {
            /*
             * Build a Var for the column to be updated.
             */
            let var: *mut Var;

            var = makeVar(
                (*(*pstate).p_target_nsitem).p_rtindex,
                attrno as i16,
                attrtype,
                attrtypmod,
                attrcollation,
                0,
            );
            (*var).location = location;

            colVar = var as *mut Node;
        }

        expr = transformAssignmentIndirection(
            pstate,
            colVar,
            colname,
            false,
            attrtype,
            attrtypmod,
            attrcollation,
            indirection,
            list_head(indirection),
            expr as *mut Node,
            COERCION_ASSIGNMENT,
            location,
        ) as *mut Expr;
    } else {
        /*
         * For normal non-qualified target column, do type checking and
         * coercion.
         */
        let orig_expr: *mut Node = expr as *mut Node;

        expr = coerce_to_target_type(
            pstate,
            orig_expr,
            type_id,
            attrtype,
            attrtypmod,
            COERCION_ASSIGNMENT,
            COERCE_IMPLICIT_CAST,
            -1,
        ) as *mut Expr;
        if expr.is_null() {
            ereport!(ERROR,
                errmsg!("column \"{}\" is of type {} but expression is of type {}",
                    std::ffi::CStr::from_ptr(colname).to_string_lossy(),
                    std::ffi::CStr::from_ptr(format_type_be(attrtype)).to_string_lossy(),
                    std::ffi::CStr::from_ptr(format_type_be(type_id)).to_string_lossy())
                /* C also: errcode(ERRCODE_DATATYPE_MISMATCH),
                   errhint("You will need to rewrite or cast the expression."),
                   parser_errposition(pstate, exprLocation(orig_expr)) */
            );
        }
    }

    (*pstate).p_expr_kind = sv_expr_kind;

    expr
}

/*
 * updateTargetListEntry()
 *	This is used in UPDATE statements (and ON CONFLICT DO UPDATE) only.
 */
pub unsafe fn updateTargetListEntry(
    pstate: *mut ParseState,
    tle: *mut TargetEntry,
    colname: *mut c_char,
    attrno: c_int,
    indirection: *mut List,
    location: c_int,
) {
    /* Fix up expression as needed */
    (*tle).expr = transformAssignedExpr(
        pstate,
        (*tle).expr,
        EXPR_KIND_UPDATE_TARGET,
        colname,
        attrno,
        indirection,
        location,
    );

    /*
     * Set the resno to identify the target column.
     */
    (*tle).resno = attrno as AttrNumber;
    (*tle).resname = colname;
}

/*
 * transformAssignmentIndirection -
 *		Process indirection (field selection or subscripting) of the target
 *		column in INSERT/UPDATE/assignment.
 */
pub unsafe fn transformAssignmentIndirection(
    pstate: *mut ParseState,
    mut basenode: *mut Node,
    targetName: *const c_char,
    targetIsSubscripting: bool,
    targetTypeId: Oid,
    targetTypMod: int32,
    targetCollation: Oid,
    indirection: *mut List,
    indirection_cell: *mut ListCell,
    rhs: *mut Node,
    ccontext: CoercionContext,
    location: c_int,
) -> *mut Node {
    let mut result: *mut Node;
    let mut subscripts: *mut List = NIL;
    let mut i: *mut ListCell;
    let mut rhs = rhs;
    let mut targetTypeId = targetTypeId;
    let mut targetTypMod = targetTypMod;
    let mut targetCollation = targetCollation;

    if !indirection_cell.is_null() && basenode.is_null() {
        /*
         * Set up a substitution.  We abuse CaseTestExpr for this.
         */
        let ctest: *mut CaseTestExpr = makeNode!(CaseTestExpr, T_CaseTestExpr);

        (*ctest).typeId = targetTypeId;
        (*ctest).typeMod = targetTypMod;
        (*ctest).collation = targetCollation;
        basenode = ctest as *mut Node;
    }

    /*
     * We have to split any field-selection operations apart from
     * subscripting.
     */
    // for_each_cell(i, indirection, indirection_cell)
    i = indirection_cell;
    while !i.is_null() {
        let n: *mut Node = lfirst(i) as *mut Node;

        if IsA!(n, T_A_Indices) {
            subscripts = lappend(subscripts, n as *mut c_void);
        } else if IsA!(n, T_A_Star) {
            ereport!(ERROR,
                errmsg!("row expansion via \"*\" is not supported here")
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                   parser_errposition(pstate, location) */
            );
        } else {
            let fstore: *mut FieldStore;
            let mut baseTypeId: Oid;
            let mut baseTypeMod: int32;
            let typrelid: Oid;
            let mut attnum: AttrNumber;
            let mut fieldTypeId: Oid = 0;
            let mut fieldTypMod: int32 = 0;
            let mut fieldCollation: Oid = 0;

            assert!(IsA!(n, T_String));

            /* process subscripts before this field selection */
            if !subscripts.is_null() {
                /* recurse, and then return because we're done */
                return transformAssignmentSubscripts(
                    pstate,
                    basenode,
                    targetName,
                    targetTypeId,
                    targetTypMod,
                    targetCollation,
                    subscripts,
                    indirection,
                    i,
                    rhs,
                    ccontext,
                    location,
                );
            }

            /* No subscripts, so can process field selection here */

            /*
             * Look up the composite type, accounting for possibility that
             * what we are given is a domain over composite.
             */
            baseTypeMod = targetTypMod;
            baseTypeId = getBaseTypeAndTypmod(targetTypeId, &mut baseTypeMod);

            typrelid = typeidTypeRelid(baseTypeId);
            if typrelid == 0 {
                ereport!(ERROR,
                    errmsg!("cannot assign to field \"{}\" of column \"{}\" because its type {} is not a composite type",
                        std::ffi::CStr::from_ptr(strVal!(n)).to_string_lossy(),
                        std::ffi::CStr::from_ptr(targetName).to_string_lossy(),
                        std::ffi::CStr::from_ptr(format_type_be(targetTypeId)).to_string_lossy())
                    /* C also: errcode(ERRCODE_DATATYPE_MISMATCH),
                       parser_errposition(pstate, location) */
                );
            }

            attnum = get_attnum(typrelid, strVal!(n));
            if attnum == InvalidAttrNumber {
                ereport!(ERROR,
                    errmsg!("cannot assign to field \"{}\" of column \"{}\" because there is no such column in data type {}",
                        std::ffi::CStr::from_ptr(strVal!(n)).to_string_lossy(),
                        std::ffi::CStr::from_ptr(targetName).to_string_lossy(),
                        std::ffi::CStr::from_ptr(format_type_be(targetTypeId)).to_string_lossy())
                    /* C also: errcode(ERRCODE_UNDEFINED_COLUMN),
                       parser_errposition(pstate, location) */
                );
            }
            if attnum < 0 {
                ereport!(ERROR,
                    errmsg!("cannot assign to system column \"{}\"",
                        std::ffi::CStr::from_ptr(strVal!(n)).to_string_lossy())
                    /* C also: errcode(ERRCODE_UNDEFINED_COLUMN),
                       parser_errposition(pstate, location) */
                );
            }

            get_atttypetypmodcoll(typrelid, attnum, &mut fieldTypeId, &mut fieldTypMod, &mut fieldCollation);

            /* recurse to create appropriate RHS for field assign */
            rhs = transformAssignmentIndirection(
                pstate,
                core::ptr::null_mut(),
                strVal!(n),
                false,
                fieldTypeId,
                fieldTypMod,
                fieldCollation,
                indirection,
                lnext(indirection, i),
                rhs,
                ccontext,
                location,
            );

            /* and build a FieldStore node */
            fstore = makeNode!(FieldStore, T_FieldStore);
            (*fstore).arg = basenode as *mut Expr;
            (*fstore).newvals = list_make1!(rhs as *mut c_void);
            (*fstore).fieldnums = list_make1_int!(attnum as c_int);
            (*fstore).resulttype = baseTypeId;

            /*
             * If target is a domain, apply constraints.
             */
            if baseTypeId != targetTypeId {
                return coerce_to_domain(
                    fstore as *mut Node,
                    baseTypeId,
                    baseTypeMod,
                    targetTypeId,
                    COERCION_IMPLICIT,
                    COERCE_IMPLICIT_CAST,
                    location,
                    false,
                );
            }

            return fstore as *mut Node;
        }

        i = lnext(indirection, i);
    }

    /* process trailing subscripts, if any */
    if !subscripts.is_null() {
        /* recurse, and then return because we're done */
        return transformAssignmentSubscripts(
            pstate,
            basenode,
            targetName,
            targetTypeId,
            targetTypMod,
            targetCollation,
            subscripts,
            indirection,
            core::ptr::null_mut(),
            rhs,
            ccontext,
            location,
        );
    }

    /* base case: just coerce RHS to match target type ID */

    result = coerce_to_target_type(
        pstate,
        rhs,
        exprType(rhs),
        targetTypeId,
        targetTypMod,
        ccontext,
        COERCE_IMPLICIT_CAST,
        -1,
    );
    if result.is_null() {
        if targetIsSubscripting {
            ereport!(ERROR,
                errmsg!("subscripted assignment to \"{}\" requires type {} but expression is of type {}",
                    std::ffi::CStr::from_ptr(targetName).to_string_lossy(),
                    std::ffi::CStr::from_ptr(format_type_be(targetTypeId)).to_string_lossy(),
                    std::ffi::CStr::from_ptr(format_type_be(exprType(rhs))).to_string_lossy())
                /* C also: errcode(ERRCODE_DATATYPE_MISMATCH),
                   errhint("You will need to rewrite or cast the expression."),
                   parser_errposition(pstate, location) */
            );
        } else {
            ereport!(ERROR,
                errmsg!("subfield \"{}\" is of type {} but expression is of type {}",
                    std::ffi::CStr::from_ptr(targetName).to_string_lossy(),
                    std::ffi::CStr::from_ptr(format_type_be(targetTypeId)).to_string_lossy(),
                    std::ffi::CStr::from_ptr(format_type_be(exprType(rhs))).to_string_lossy())
                /* C also: errcode(ERRCODE_DATATYPE_MISMATCH),
                   errhint("You will need to rewrite or cast the expression."),
                   parser_errposition(pstate, location) */
            );
        }
    }

    result
}

/*
 * helper for transformAssignmentIndirection: process container assignment
 */
unsafe fn transformAssignmentSubscripts(
    pstate: *mut ParseState,
    basenode: *mut Node,
    targetName: *const c_char,
    targetTypeId: Oid,
    targetTypMod: int32,
    targetCollation: Oid,
    subscripts: *mut List,
    indirection: *mut List,
    next_indirection: *mut ListCell,
    mut rhs: *mut Node,
    ccontext: CoercionContext,
    location: c_int,
) -> *mut Node {
    let mut result: *mut Node;
    let sbsref: *mut SubscriptingRef;
    let mut containerType: Oid;
    let mut containerTypMod: int32;
    let typeNeeded: Oid;
    let typmodNeeded: int32;
    let collationNeeded: Oid;

    assert!(!subscripts.is_null());

    /* Identify the actual container type involved */
    containerType = targetTypeId;
    containerTypMod = targetTypMod;
    transformContainerType(&mut containerType, &mut containerTypMod);

    /* Process subscripts and identify required type for RHS */
    sbsref = transformContainerSubscripts(
        pstate,
        basenode,
        containerType,
        containerTypMod,
        subscripts,
        true,
    );

    typeNeeded = (*sbsref).refrestype;
    typmodNeeded = (*sbsref).reftypmod;

    /*
     * Container normally has same collation as its elements, but there's an
     * exception: we might be subscripting a domain over a container type.
     */
    if containerType == targetTypeId {
        collationNeeded = targetCollation;
    } else {
        collationNeeded = get_typcollation(containerType);
    }

    /* recurse to create appropriate RHS for container assign */
    rhs = transformAssignmentIndirection(
        pstate,
        core::ptr::null_mut(),
        targetName,
        true,
        typeNeeded,
        typmodNeeded,
        collationNeeded,
        indirection,
        next_indirection,
        rhs,
        ccontext,
        location,
    );

    /*
     * Insert the already-properly-coerced RHS into the SubscriptingRef.
     */
    (*sbsref).refassgnexpr = rhs as *mut Expr;
    (*sbsref).refrestype = containerType;
    (*sbsref).reftypmod = containerTypMod;

    result = sbsref as *mut Node;

    /*
     * If target was a domain over container, need to coerce up to the domain.
     */
    if containerType != targetTypeId {
        let resulttype: Oid = exprType(result);

        result = coerce_to_target_type(
            pstate,
            result,
            resulttype,
            targetTypeId,
            targetTypMod,
            ccontext,
            COERCE_IMPLICIT_CAST,
            -1,
        );
        /* can fail if we had int2vector/oidvector, but not for true domains */
        if result.is_null() {
            ereport!(ERROR,
                errmsg!("cannot cast type {} to {}",
                    std::ffi::CStr::from_ptr(format_type_be(resulttype)).to_string_lossy(),
                    std::ffi::CStr::from_ptr(format_type_be(targetTypeId)).to_string_lossy())
                /* C also: errcode(ERRCODE_CANNOT_COERCE),
                   parser_errposition(pstate, location) */
            );
        }
    }

    result
}

/*
 * checkInsertTargets -
 *	  generate a list of INSERT column targets if not supplied, or
 *	  test supplied column names to make sure they are in target table.
 *	  Also return an integer list of the columns' attribute numbers.
 */
pub unsafe fn checkInsertTargets(
    pstate: *mut ParseState,
    mut cols: *mut List,
    attrnos: *mut *mut List,
) -> *mut List {
    *attrnos = NIL;

    if cols.is_null() {
        /*
         * Generate default column list for INSERT.
         */
        use crate::utils::rel::RelationGetNumberOfAttributes;
        use crate::utils::rel::RelationData;
        let numcol: c_int = RelationGetNumberOfAttributes((*pstate).p_target_relation as *mut RelationData);
        let mut i: c_int;

        i = 0;
        while i < numcol {
            let col: *mut ResTarget;
            let attr: Form_pg_attribute;

            attr = TupleDescAttr((*((*pstate).p_target_relation as *mut RelationData)).rd_att, i);

            if (*attr).attisdropped {
                i += 1;
                continue;
            }

            col = makeNode!(ResTarget, T_ResTarget);
            (*col).name = pstrdup(NameStr(&(*attr).attname));
            (*col).indirection = NIL;
            (*col).val = core::ptr::null_mut();
            (*col).location = -1;
            cols = lappend(cols, col as *mut c_void);
            *attrnos = lappend_int(*attrnos, i + 1);

            i += 1;
        }
    } else {
        /*
         * Do initial validation of user-supplied INSERT column list.
         */
        let mut wholecols: *mut Bitmapset = core::ptr::null_mut();
        let mut partialcols: *mut Bitmapset = core::ptr::null_mut();
        let mut tl: *mut ListCell;

        foreach!(tl, cols, {
            let col: *mut ResTarget = lfirst(current_cell!(tl)) as *mut ResTarget;
            let name: *mut c_char = (*col).name;
            let attrno: c_int;

            /* Lookup column name, ereport on failure */
            attrno = attnameAttNum((*pstate).p_target_relation, name, false);
            if attrno == InvalidAttrNumber as c_int {
                use crate::utils::rel::{RelationGetRelationName, RelationData};
                ereport!(ERROR,
                    errmsg!("column \"{}\" of relation \"{}\" does not exist",
                        std::ffi::CStr::from_ptr(name).to_string_lossy(),
                        std::ffi::CStr::from_ptr(RelationGetRelationName((*pstate).p_target_relation as *mut RelationData)).to_string_lossy())
                    /* C also: errcode(ERRCODE_UNDEFINED_COLUMN),
                       parser_errposition(pstate, (*col).location) */
                );
            }

            /*
             * Check for duplicates, but only of whole columns.
             */
            if (*col).indirection.is_null() {
                /* whole column; must not have any other assignment */
                if bms_is_member(attrno, wholecols) || bms_is_member(attrno, partialcols) {
                    ereport!(ERROR,
                        errmsg!("column \"{}\" specified more than once",
                            std::ffi::CStr::from_ptr(name).to_string_lossy())
                        /* C also: errcode(ERRCODE_DUPLICATE_COLUMN),
                           parser_errposition(pstate, (*col).location) */
                    );
                }
                wholecols = bms_add_member(wholecols, attrno);
            } else {
                /* partial column; must not have any whole assignment */
                if bms_is_member(attrno, wholecols) {
                    ereport!(ERROR,
                        errmsg!("column \"{}\" specified more than once",
                            std::ffi::CStr::from_ptr(name).to_string_lossy())
                        /* C also: errcode(ERRCODE_DUPLICATE_COLUMN),
                           parser_errposition(pstate, (*col).location) */
                    );
                }
                partialcols = bms_add_member(partialcols, attrno);
            }

            *attrnos = lappend_int(*attrnos, attrno);
        });
    }

    cols
}

/*
 * ExpandColumnRefStar()
 *		Transforms foo.* into a list of expressions or targetlist entries.
 */
unsafe fn ExpandColumnRefStar(
    pstate: *mut ParseState,
    cref: *mut ColumnRef,
    make_target_entry: bool,
) -> *mut List {
    let fields: *mut List = (*cref).fields;
    let numnames: c_int = list_length(fields);

    if numnames == 1 {
        /*
         * Target item is a bare '*', expand all tables
         */
        assert!(make_target_entry);
        return ExpandAllTables(pstate, (*cref).location);
    } else {
        /*
         * Target item is relation.*, expand that table
         */
        let mut nspname: *mut c_char = core::ptr::null_mut();
        let mut relname: *mut c_char = core::ptr::null_mut();
        let mut nsitem: *mut ParseNamespaceItem = core::ptr::null_mut();
        let mut levels_up: c_int = 0;

        #[repr(C)]
        enum CrsErr { CrsErrNoRte, CrsErrWrongDb, CrsErrTooMany }
        let mut crserr = CrsErr::CrsErrNoRte;

        /*
         * Give the PreParseColumnRefHook, if any, first shot.
         */
        if (*pstate).p_pre_columnref_hook.is_some() {
            let node = ((*pstate).p_pre_columnref_hook.unwrap())(pstate, cref as *mut c_void);
            if !node.is_null() {
                return ExpandRowReference(pstate, node, make_target_entry);
            }
        }

        match numnames {
            2 => {
                relname = strVal!(linitial(fields));
                nsitem = refnameNamespaceItem(pstate, nspname, relname, (*cref).location, &mut levels_up);
            }
            3 => {
                nspname = strVal!(linitial(fields));
                relname = strVal!(lsecond(fields));
                nsitem = refnameNamespaceItem(pstate, nspname, relname, (*cref).location, &mut levels_up);
            }
            4 => {
                let catname: *mut c_char = strVal!(linitial(fields));
                /*
                 * We check the catalog name and then ignore it.
                 */
                if libc_strcmp(catname, get_database_name(MyDatabaseId)) != 0 {
                    crserr = CrsErr::CrsErrWrongDb;
                } else {
                    nspname = strVal!(lsecond(fields));
                    relname = strVal!(lthird(fields));
                    nsitem = refnameNamespaceItem(pstate, nspname, relname, (*cref).location, &mut levels_up);
                }
            }
            _ => {
                crserr = CrsErr::CrsErrTooMany;
            }
        }

        /*
         * Now give the PostParseColumnRefHook, if any, a chance.
         */
        if (*pstate).p_post_columnref_hook.is_some() {
            let rte_or_null = if !nsitem.is_null() { (*nsitem).p_rte } else { core::ptr::null_mut() };
            let node = ((*pstate).p_post_columnref_hook.unwrap())(pstate, cref as *mut c_void, rte_or_null as *mut Node);
            if !node.is_null() {
                if !nsitem.is_null() {
                    ereport!(ERROR,
                        errmsg!("column reference \"{}\" is ambiguous",
                            std::ffi::CStr::from_ptr(NameListToString((*cref).fields)).to_string_lossy())
                        /* C also: errcode(ERRCODE_AMBIGUOUS_COLUMN),
                           parser_errposition(pstate, (*cref).location) */
                    );
                }
                return ExpandRowReference(pstate, node, make_target_entry);
            }
        }

        /*
         * Throw error if no translation found.
         */
        if nsitem.is_null() {
            match crserr {
                CrsErr::CrsErrNoRte => {
                    errorMissingRTE(pstate, makeRangeVar(nspname, relname, (*cref).location));
                }
                CrsErr::CrsErrWrongDb => {
                    ereport!(ERROR,
                        errmsg!("cross-database references are not implemented: {}",
                            std::ffi::CStr::from_ptr(NameListToString((*cref).fields)).to_string_lossy())
                        /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                           parser_errposition(pstate, (*cref).location) */
                    );
                }
                CrsErr::CrsErrTooMany => {
                    ereport!(ERROR,
                        errmsg!("improper qualified name (too many dotted names): {}",
                            std::ffi::CStr::from_ptr(NameListToString((*cref).fields)).to_string_lossy())
                        /* C also: errcode(ERRCODE_SYNTAX_ERROR),
                           parser_errposition(pstate, (*cref).location) */
                    );
                }
            }
        }

        /*
         * OK, expand the nsitem into fields.
         */
        ExpandSingleTable(pstate, nsitem, levels_up, (*cref).location, make_target_entry)
    }
}

/*
 * ExpandAllTables()
 *		Transforms '*' (in the target list) into a list of targetlist entries.
 */
unsafe fn ExpandAllTables(pstate: *mut ParseState, location: c_int) -> *mut List {
    let mut target: *mut List = NIL;
    let mut found_table: bool = false;
    let mut l: *mut ListCell;

    foreach!(l, (*pstate).p_namespace, {
        let nsitem: *mut ParseNamespaceItem = lfirst(current_cell!(l)) as *mut ParseNamespaceItem;

        /* Ignore table-only items */
        if !(*nsitem).p_cols_visible {
            continue;
        }
        /* Should not have any lateral-only items when parsing targetlist */
        assert!(!(*nsitem).p_lateral_only);
        /* Remember we found a p_cols_visible item */
        found_table = true;

        target = list_concat(
            target,
            expandNSItemAttrs(pstate, nsitem, 0, true, location),
        );
    });

    /*
     * Check for "SELECT *;".
     */
    if !found_table {
        ereport!(ERROR,
            errmsg!("SELECT * with no tables specified is not valid")
            /* C also: errcode(ERRCODE_SYNTAX_ERROR),
               parser_errposition(pstate, location) */
        );
    }

    target
}

/*
 * ExpandIndirectionStar()
 *		Transforms foo.* into a list of expressions or targetlist entries.
 */
unsafe fn ExpandIndirectionStar(
    pstate: *mut ParseState,
    mut ind: *mut A_Indirection,
    make_target_entry: bool,
    exprKind: ParseExprKind,
) -> *mut List {
    let expr: *mut Node;

    /* Strip off the '*' to create a reference to the rowtype object */
    // TODO(pg-port): copyObject -- shallow copy for now
    ind = copyObject(ind);
    (*ind).indirection = list_truncate(
        (*ind).indirection,
        list_length((*ind).indirection) - 1,
    );

    /* And transform that */
    expr = transformExpr(pstate, ind as *mut Node, exprKind);

    /* Expand the rowtype expression into individual fields */
    ExpandRowReference(pstate, expr, make_target_entry)
}

// TODO(pg-port): copyfuncs.c copyObject
unsafe fn copyObject<T>(node: *mut T) -> *mut T {
    node
}

/*
 * ExpandSingleTable()
 *		Transforms foo.* into a list of expressions or targetlist entries.
 */
unsafe fn ExpandSingleTable(
    pstate: *mut ParseState,
    nsitem: *mut ParseNamespaceItem,
    sublevels_up: c_int,
    location: c_int,
    make_target_entry: bool,
) -> *mut List {
    if make_target_entry {
        /* expandNSItemAttrs handles permissions marking */
        return expandNSItemAttrs(pstate, nsitem, sublevels_up, true, location);
    } else {
        let rte: *mut RangeTblEntry = (*nsitem).p_rte as *mut RangeTblEntry;
        let perminfo: *mut RTEPermissionInfo = (*nsitem).p_perminfo as *mut RTEPermissionInfo;
        let vars: *mut List;
        let mut l: *mut ListCell;

        vars = expandNSItemVars(pstate, nsitem, sublevels_up, location, core::ptr::null_mut());

        /*
         * Require read access to the table.
         */
        if (*rte).rtekind == RTE_RELATION {
            assert!(!perminfo.is_null());
            (*perminfo).requiredPerms |= ACL_SELECT;
        }

        /* Require read access to each column */
        foreach!(l, vars, {
            let var: *mut Var = lfirst(current_cell!(l)) as *mut Var;
            markVarForSelectPriv(pstate, var);
        });

        vars
    }
}

/*
 * ExpandRowReference()
 *		Transforms foo.* into a list of expressions or targetlist entries.
 */
unsafe fn ExpandRowReference(
    pstate: *mut ParseState,
    expr: *mut Node,
    make_target_entry: bool,
) -> *mut List {
    let mut result: *mut List = NIL;
    let tupleDesc: TupleDesc;
    let numAttrs: c_int;
    let mut i: c_int;

    /*
     * If the rowtype expression is a whole-row Var, we can expand the fields
     * as simple Vars.
     */
    if IsA!(expr, T_Var) && (*(expr as *mut Var)).varattno == InvalidAttrNumber {
        let var: *mut Var = expr as *mut Var;
        let nsitem: *mut ParseNamespaceItem;

        nsitem = GetNSItemByRangeTablePosn(pstate, (*var).varno, (*var).varlevelsup as c_int);
        return ExpandSingleTable(pstate, nsitem, (*var).varlevelsup as c_int, (*var).location, make_target_entry);
    }

    /*
     * Otherwise we have to do it the hard way.
     *
     * If it's a Var of type RECORD, we have to work even harder.
     */
    if IsA!(expr, T_Var) && (*(expr as *mut Var)).vartype == RECORDOID {
        tupleDesc = expandRecordVariable(pstate, expr as *mut Var, 0);
    } else {
        tupleDesc = get_expr_result_tupdesc(expr, false);
    }
    assert!(!tupleDesc.is_null());

    /* Generate a list of references to the individual fields */
    numAttrs = (*tupleDesc).natts;
    i = 0;
    while i < numAttrs {
        let att: Form_pg_attribute = TupleDescAttr(tupleDesc, i);
        let fselect: *mut FieldSelect;

        if (*att).attisdropped {
            i += 1;
            continue;
        }

        fselect = makeNode!(FieldSelect, T_FieldSelect);
        (*fselect).arg = copyObject(expr) as *mut Expr;
        (*fselect).fieldnum = (i + 1) as AttrNumber;
        (*fselect).resulttype = (*att).atttypid;
        (*fselect).resulttypmod = (*att).atttypmod;
        /* save attribute's collation for parse_collate.c */
        (*fselect).resultcollid = (*att).attcollation;

        if make_target_entry {
            /* add TargetEntry decoration */
            let te: *mut TargetEntry;

            te = makeTargetEntry(
                fselect as *mut Expr,
                (*pstate).p_next_resno as AttrNumber,
                pstrdup(NameStr(&(*att).attname)),
                false,
            );
            (*pstate).p_next_resno += 1;
            result = lappend(result, te as *mut c_void);
        } else {
            result = lappend(result, fselect as *mut c_void);
        }

        i += 1;
    }

    result
}

/*
 * expandRecordVariable
 *		Get the tuple descriptor for a Var of type RECORD, if possible.
 */
pub unsafe fn expandRecordVariable(
    pstate: *mut ParseState,
    var: *mut Var,
    levelsup: c_int,
) -> TupleDesc {
    let tupleDesc: TupleDesc;
    let netlevelsup: c_int;
    let rte: *mut RangeTblEntry;
    let attnum: AttrNumber;
    let mut expr: *mut Node;

    /* Check my caller didn't mess up */
    assert!(IsA!(var as *mut Node, T_Var));
    assert!((*(var as *mut Var)).vartype == RECORDOID);

    netlevelsup = (*var).varlevelsup as c_int + levelsup;
    rte = GetRTEByRangeTablePosn(pstate, (*var).varno, netlevelsup);
    attnum = (*var).varattno;

    if attnum == InvalidAttrNumber {
        /* Whole-row reference to an RTE, so expand the known fields */
        let mut names: *mut List = NIL;
        let mut vars: *mut List = NIL;
        let mut lname: *mut ListCell;
        let mut lvar: *mut ListCell;
        let mut i: c_int;

        expandRTE(
            rte,
            (*var).varno,
            0,
            (*var).varreturningtype,
            (*var).location,
            false,
            &mut names,
            &mut vars,
        );

        let tup_desc = CreateTemplateTupleDesc(list_length(vars));
        i = 1;
        lname = list_head(names);
        lvar = list_head(vars);
        while !lname.is_null() && !lvar.is_null() {
            let label: *mut c_char = strVal!(lfirst(lname));
            let varnode: *mut Node = lfirst(lvar) as *mut Node;

            TupleDescInitEntry(tup_desc, i as AttrNumber, label, exprType(varnode), exprTypmod(varnode), 0);
            TupleDescInitEntryCollation(tup_desc, i as AttrNumber, exprCollation(varnode));
            i += 1;

            lname = lnext(names, lname);
            lvar = lnext(vars, lvar);
        }
        assert!(lname.is_null() && lvar.is_null()); /* lists same length? */

        return tup_desc;
    }

    expr = var as *mut Node; /* default if we can't drill down */

    match (*rte).rtekind {
        RTE_RELATION | RTE_VALUES | RTE_NAMEDTUPLESTORE | RTE_RESULT => {
            /*
             * This case should not occur: a column of a table, values list,
             * or ENR shouldn't have type RECORD.  Fall through and fail.
             */
        }
        RTE_SUBQUERY => {
            /* Subselect-in-FROM: examine sub-select's output expr */
            let ste: *mut TargetEntry =
                get_tle_by_resno((*(*rte).subquery).targetList, attnum);

            if ste.is_null() || (*ste).resjunk {
                panic!(
                    "subquery {} does not have attribute {}",
                    std::ffi::CStr::from_ptr((*(*rte).eref).aliasname).to_string_lossy(),
                    attnum
                );
            }
            expr = (*ste).expr as *mut Node;
            if IsA!(expr, T_Var) {
                /*
                 * Recurse into the sub-select to see what its Var refers to.
                 */
                let mut mypstate: ParseState = core::mem::zeroed();
                let mut local_levelsup: Index;

                /* this loop must work, since GetRTEByRangeTablePosn did */
                let mut cur_pstate = pstate;
                local_levelsup = 0;
                while local_levelsup < netlevelsup as Index {
                    cur_pstate = (*cur_pstate).parentParseState;
                    local_levelsup += 1;
                }
                mypstate.parentParseState = cur_pstate;
                mypstate.p_rtable = (*(*rte).subquery).rtable;
                /* don't bother filling the rest of the fake pstate */

                return expandRecordVariable(&mut mypstate, expr as *mut Var, 0);
            }
            /* else fall through to inspect the expression */
        }
        RTE_JOIN => {
            /* Join RTE --- recursively inspect the alias variable */
            assert!(attnum > 0 && (attnum as c_int) <= list_length((*rte).joinaliasvars));
            expr = list_nth((*rte).joinaliasvars, (attnum - 1) as c_int) as *mut Node;
            assert!(!expr.is_null());
            /* We intentionally don't strip implicit coercions here */
            if IsA!(expr, T_Var) {
                return expandRecordVariable(pstate, expr as *mut Var, netlevelsup);
            }
            /* else fall through to inspect the expression */
        }
        RTE_FUNCTION => {
            /*
             * We couldn't get here unless a function is declared with one of
             * its result columns as RECORD, which is not allowed.
             */
        }
        RTE_TABLEFUNC => {
            /*
             * Table function cannot have columns with RECORD type.
             */
        }
        RTE_CTE => {
            /* CTE reference: examine subquery's output expr */
            if !(*rte).self_reference {
                let cte: *mut CommonTableExpr = GetCTEForRTE(pstate, rte, netlevelsup);
                let ste: *mut TargetEntry;

                ste = get_tle_by_resno(GetCTETargetList(cte), attnum);
                if ste.is_null() || (*ste).resjunk {
                    panic!(
                        "CTE {} does not have attribute {}",
                        std::ffi::CStr::from_ptr((*(*rte).eref).aliasname).to_string_lossy(),
                        attnum
                    );
                }
                expr = (*ste).expr as *mut Node;
                if IsA!(expr, T_Var) {
                    /*
                     * Recurse into the CTE to see what its Var refers to.
                     */
                    let mut mypstate: ParseState = core::mem::zeroed();
                    let mut local_levelsup: Index;

                    let combined_levelsup = (*rte).ctelevelsup as c_int + netlevelsup;
                    let mut cur_pstate = pstate;
                    local_levelsup = 0;
                    while (local_levelsup as c_int) < combined_levelsup {
                        cur_pstate = (*cur_pstate).parentParseState;
                        local_levelsup += 1;
                    }
                    mypstate.parentParseState = cur_pstate;
                    mypstate.p_rtable = (*((*cte).ctequery as *mut Query)).rtable;
                    /* don't bother filling the rest of the fake pstate */

                    return expandRecordVariable(&mut mypstate, expr as *mut Var, 0);
                }
                /* else fall through to inspect the expression */
            }
        }
        RTE_GROUP => {
            /*
             * We couldn't get here: the RTE_GROUP RTE has not been added.
             */
        }
        _ => {}
    }

    /*
     * We now have an expression we can't expand any more, so see if
     * get_expr_result_tupdesc() can do anything with it.
     */
    get_expr_result_tupdesc(expr, false)
}

/*
 * FigureColname -
 *	  if the name of the resulting column is not specified in the target
 *	  list, we have to guess a suitable name.
 */
pub unsafe fn FigureColname(node: *mut Node) -> *mut c_char {
    let mut name: *mut c_char = core::ptr::null_mut();

    FigureColnameInternal(node, &mut name);
    if !name.is_null() {
        return name;
    }
    /* default result if we can't guess anything */
    b"?column?\0".as_ptr() as *mut c_char
}

/*
 * FigureIndexColname -
 *	  choose the name for an expression column in an index
 *
 * This is actually just like FigureColname, except we return NULL if
 * we can't pick a good name.
 */
pub unsafe fn FigureIndexColname(node: *mut Node) -> *mut c_char {
    let mut name: *mut c_char = core::ptr::null_mut();

    FigureColnameInternal(node, &mut name);
    name
}

/*
 * FigureColnameInternal -
 *	  internal workhorse for FigureColname
 *
 * Return value indicates strength of confidence in result:
 *		0 - no information
 *		1 - second-best name choice
 *		2 - good name choice
 */
unsafe fn FigureColnameInternal(node: *mut Node, name: *mut *mut c_char) -> c_int {
    let mut strength: c_int = 0;

    if node.is_null() {
        return strength;
    }

    match nodeTag(node) {
        T_ColumnRef => {
            let mut fname: *mut c_char = core::ptr::null_mut();
            let mut l: *mut ListCell;

            /* find last field name, if any, ignoring "*" */
            foreach!(l, (*(node as *mut ColumnRef)).fields, {
                let i: *mut Node = lfirst(current_cell!(l)) as *mut Node;

                if IsA!(i, T_String) {
                    fname = strVal!(i);
                }
            });
            if !fname.is_null() {
                *name = fname;
                return 2;
            }
        }
        T_A_Indirection => {
            let ind: *mut A_Indirection = node as *mut A_Indirection;
            let mut fname: *mut c_char = core::ptr::null_mut();
            let mut l: *mut ListCell;

            /* find last field name, if any, ignoring "*" and subscripts */
            foreach!(l, (*ind).indirection, {
                let i: *mut Node = lfirst(current_cell!(l)) as *mut Node;

                if IsA!(i, T_String) {
                    fname = strVal!(i);
                }
            });
            if !fname.is_null() {
                *name = fname;
                return 2;
            }
            return FigureColnameInternal((*ind).arg, name);
        }
        T_FuncCall => {
            *name = strVal!(llast((*(node as *mut FuncCall)).funcname));
            return 2;
        }
        T_A_Expr => {
            if (*(node as *mut A_Expr)).kind == A_ExprKind::AEXPR_NULLIF {
                /* make nullif() act like a regular function */
                *name = b"nullif\0".as_ptr() as *mut c_char;
                return 2;
            }
        }
        T_TypeCast => {
            strength = FigureColnameInternal((*(node as *mut TypeCast)).arg, name);
            if strength <= 1 {
                if !(*(node as *mut TypeCast)).typeName.is_null() {
                    *name = strVal!(llast((*(*(node as *mut TypeCast)).typeName).names));
                    return 1;
                }
            }
        }
        T_CollateClause => {
            return FigureColnameInternal((*(node as *mut CollateClause)).arg, name);
        }
        T_GroupingFunc => {
            /* make GROUPING() act like a regular function */
            *name = b"grouping\0".as_ptr() as *mut c_char;
            return 2;
        }
        T_MergeSupportFunc => {
            /* make MERGE_ACTION() act like a regular function */
            *name = b"merge_action\0".as_ptr() as *mut c_char;
            return 2;
        }
        T_SubLink => {
            match (*(node as *mut SubLink)).subLinkType {
                SubLinkType::EXISTS_SUBLINK => {
                    *name = b"exists\0".as_ptr() as *mut c_char;
                    return 2;
                }
                SubLinkType::ARRAY_SUBLINK => {
                    *name = b"array\0".as_ptr() as *mut c_char;
                    return 2;
                }
                SubLinkType::EXPR_SUBLINK => {
                    /* Get column name of the subquery's single target */
                    let sublink: *mut SubLink = node as *mut SubLink;
                    let query: *mut Query = (*sublink).subselect as *mut Query;

                    /*
                     * The subquery has probably already been transformed.
                     */
                    if IsA!(query as *mut Node, T_Query) {
                        let te: *mut TargetEntry =
                            linitial((*query).targetList) as *mut TargetEntry;

                        if !(*te).resname.is_null() {
                            *name = (*te).resname;
                            return 2;
                        }
                    }
                }
                /* As with other operator-like nodes, these have no names */
                SubLinkType::MULTIEXPR_SUBLINK
                | SubLinkType::ALL_SUBLINK
                | SubLinkType::ANY_SUBLINK
                | SubLinkType::ROWCOMPARE_SUBLINK
                | SubLinkType::CTE_SUBLINK => {}
                _ => {}
            }
        }
        T_CaseExpr => {
            strength = FigureColnameInternal(
                (*(node as *mut CaseExpr)).defresult as *mut Node,
                name,
            );
            if strength <= 1 {
                *name = b"case\0".as_ptr() as *mut c_char;
                return 1;
            }
        }
        T_A_ArrayExpr => {
            /* make ARRAY[] act like a function */
            *name = b"array\0".as_ptr() as *mut c_char;
            return 2;
        }
        T_RowExpr => {
            /* make ROW() act like a function */
            *name = b"row\0".as_ptr() as *mut c_char;
            return 2;
        }
        T_CoalesceExpr => {
            /* make coalesce() act like a regular function */
            *name = b"coalesce\0".as_ptr() as *mut c_char;
            return 2;
        }
        T_MinMaxExpr => {
            /* make greatest/least act like a regular function */
            match (*(node as *mut MinMaxExpr)).op {
                MinMaxOp::IS_GREATEST => {
                    *name = b"greatest\0".as_ptr() as *mut c_char;
                    return 2;
                }
                MinMaxOp::IS_LEAST => {
                    *name = b"least\0".as_ptr() as *mut c_char;
                    return 2;
                }
                _ => {}
            }
        }
        T_SQLValueFunction => {
            /* make these act like a function or variable */
            match (*(node as *mut SQLValueFunction)).op {
                SQLValueFunctionOp::SVFOP_CURRENT_DATE => {
                    *name = b"current_date\0".as_ptr() as *mut c_char;
                    return 2;
                }
                SQLValueFunctionOp::SVFOP_CURRENT_TIME
                | SQLValueFunctionOp::SVFOP_CURRENT_TIME_N => {
                    *name = b"current_time\0".as_ptr() as *mut c_char;
                    return 2;
                }
                SQLValueFunctionOp::SVFOP_CURRENT_TIMESTAMP
                | SQLValueFunctionOp::SVFOP_CURRENT_TIMESTAMP_N => {
                    *name = b"current_timestamp\0".as_ptr() as *mut c_char;
                    return 2;
                }
                SQLValueFunctionOp::SVFOP_LOCALTIME
                | SQLValueFunctionOp::SVFOP_LOCALTIME_N => {
                    *name = b"localtime\0".as_ptr() as *mut c_char;
                    return 2;
                }
                SQLValueFunctionOp::SVFOP_LOCALTIMESTAMP
                | SQLValueFunctionOp::SVFOP_LOCALTIMESTAMP_N => {
                    *name = b"localtimestamp\0".as_ptr() as *mut c_char;
                    return 2;
                }
                SQLValueFunctionOp::SVFOP_CURRENT_ROLE => {
                    *name = b"current_role\0".as_ptr() as *mut c_char;
                    return 2;
                }
                SQLValueFunctionOp::SVFOP_CURRENT_USER => {
                    *name = b"current_user\0".as_ptr() as *mut c_char;
                    return 2;
                }
                SQLValueFunctionOp::SVFOP_USER => {
                    *name = b"user\0".as_ptr() as *mut c_char;
                    return 2;
                }
                SQLValueFunctionOp::SVFOP_SESSION_USER => {
                    *name = b"session_user\0".as_ptr() as *mut c_char;
                    return 2;
                }
                SQLValueFunctionOp::SVFOP_CURRENT_CATALOG => {
                    *name = b"current_catalog\0".as_ptr() as *mut c_char;
                    return 2;
                }
                SQLValueFunctionOp::SVFOP_CURRENT_SCHEMA => {
                    *name = b"current_schema\0".as_ptr() as *mut c_char;
                    return 2;
                }
                _ => {}
            }
        }
        T_XmlExpr => {
            /* make SQL/XML functions act like a regular function */
            match (*(node as *mut XmlExpr)).op {
                XmlExprOp::IS_XMLCONCAT => {
                    *name = b"xmlconcat\0".as_ptr() as *mut c_char;
                    return 2;
                }
                XmlExprOp::IS_XMLELEMENT => {
                    *name = b"xmlelement\0".as_ptr() as *mut c_char;
                    return 2;
                }
                XmlExprOp::IS_XMLFOREST => {
                    *name = b"xmlforest\0".as_ptr() as *mut c_char;
                    return 2;
                }
                XmlExprOp::IS_XMLPARSE => {
                    *name = b"xmlparse\0".as_ptr() as *mut c_char;
                    return 2;
                }
                XmlExprOp::IS_XMLPI => {
                    *name = b"xmlpi\0".as_ptr() as *mut c_char;
                    return 2;
                }
                XmlExprOp::IS_XMLROOT => {
                    *name = b"xmlroot\0".as_ptr() as *mut c_char;
                    return 2;
                }
                XmlExprOp::IS_XMLSERIALIZE => {
                    *name = b"xmlserialize\0".as_ptr() as *mut c_char;
                    return 2;
                }
                XmlExprOp::IS_DOCUMENT => {
                    /* nothing */
                }
                _ => {}
            }
        }
        T_XmlSerialize => {
            /* make XMLSERIALIZE act like a regular function */
            *name = b"xmlserialize\0".as_ptr() as *mut c_char;
            return 2;
        }
        T_JsonParseExpr => {
            /* make JSON act like a regular function */
            *name = b"json\0".as_ptr() as *mut c_char;
            return 2;
        }
        T_JsonScalarExpr => {
            /* make JSON_SCALAR act like a regular function */
            *name = b"json_scalar\0".as_ptr() as *mut c_char;
            return 2;
        }
        T_JsonSerializeExpr => {
            /* make JSON_SERIALIZE act like a regular function */
            *name = b"json_serialize\0".as_ptr() as *mut c_char;
            return 2;
        }
        T_JsonObjectConstructor => {
            /* make JSON_OBJECT act like a regular function */
            *name = b"json_object\0".as_ptr() as *mut c_char;
            return 2;
        }
        T_JsonArrayConstructor | T_JsonArrayQueryConstructor => {
            /* make JSON_ARRAY act like a regular function */
            *name = b"json_array\0".as_ptr() as *mut c_char;
            return 2;
        }
        T_JsonObjectAgg => {
            /* make JSON_OBJECTAGG act like a regular function */
            *name = b"json_objectagg\0".as_ptr() as *mut c_char;
            return 2;
        }
        T_JsonArrayAgg => {
            /* make JSON_ARRAYAGG act like a regular function */
            *name = b"json_arrayagg\0".as_ptr() as *mut c_char;
            return 2;
        }
        T_JsonFuncExpr => {
            /* make SQL/JSON functions act like a regular function */
            match (*(node as *mut JsonFuncExpr)).op {
                JsonExprOp::JSON_EXISTS_OP => {
                    *name = b"json_exists\0".as_ptr() as *mut c_char;
                    return 2;
                }
                JsonExprOp::JSON_QUERY_OP => {
                    *name = b"json_query\0".as_ptr() as *mut c_char;
                    return 2;
                }
                JsonExprOp::JSON_VALUE_OP => {
                    *name = b"json_value\0".as_ptr() as *mut c_char;
                    return 2;
                }
                /* JSON_TABLE_OP can't happen here. */
                _ => {
                    panic!("unrecognized JsonExpr op: {}", (*(node as *mut JsonFuncExpr)).op as c_int);
                }
            }
        }
        _ => {}
    }

    strength
}

// ---------------------------------------------------------------------------
// Additional small stubs needed above
// ---------------------------------------------------------------------------
use crate::list_make1_int;
use crate::common::int::pg_cmp_s32;
