//! parse_jsontable.rs
//!   parsing of JSON_TABLE
//!
//! Translated 1:1 from postgres/src/backend/parser/parse_jsontable.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/parser/parse_jsontable.c

use crate::prelude::*;

use crate::{castNode, current_cell, foreach, list_make1, makeNode, DirectFunctionCall1, IsA};

use crate::catalog::pg_type::{TYPTYPE_COMPOSITE, TYPTYPE_DOMAIN};
use crate::catalog::pg_type_d::{INT4OID, JSONBOID, JSONOID, JSONPATHOID, RECORDOID};

use crate::lib::stringinfo::{appendStringInfoString, initStringInfo, StringInfoData};

// TODO(pg-port): real copyObjectImpl lives in nodes/copyfuncs.rs (deferred/unwired).
unsafe fn copyObjectImpl(from: *const c_void) -> *mut c_void {
    from as *mut c_void
}
use crate::nodes::makefuncs::{
    makeConst, makeJsonFormat, makeJsonTablePath, makeJsonValueExpr, makeStringConst,
};
use crate::nodes::nodeFuncs::{exprCollation, exprType, exprTypmod};
use crate::nodes::nodes::{Node, NodeTag};
use crate::nodes::parsenodes::{
    A_Const, JsonFuncExpr, JsonOutput, JsonTable, JsonTableColumn, JsonTablePathSpec,
    JTC_EXISTS, JTC_FORMATTED, JTC_FOR_ORDINALITY, JTC_NESTED, JTC_REGULAR, JS_QUOTES_UNSPEC,
};
use crate::nodes::pg_list::{lappend, lappend_int, lappend_oid, lfirst, list_length, List};
use crate::nodes::primnodes::{
    CaseTestExpr, Const, Expr, JsonExpr, JsonReturning, JsonTablePathScan, JsonTablePlan,
    JsonTableSiblingJoin, TableFunc, JSON_BEHAVIOR_EMPTY, JSON_BEHAVIOR_EMPTY_ARRAY,
    JSON_BEHAVIOR_ERROR, JSON_EXISTS_OP, JSON_QUERY_OP, JSON_TABLE_OP, JSON_VALUE_OP,
    JS_ENC_DEFAULT, JS_FORMAT_DEFAULT, JSW_UNSPEC, TFT_JSON_TABLE,
};
use crate::nodes::value::makeString;

use crate::parser::parse_collate::assign_expr_collations;
use crate::parser::parse_node::ParseExprKind::EXPR_KIND_FROM_FUNCTION;
use crate::parser::parse_node::{ParseExprKind, ParseNamespaceItem, ParseState};
use crate::parser::parse_type::typenameTypeIdAndMod;

use crate::optimizer::optimizer::contain_vars_of_level;

use crate::utils::adt::json::escape_json;
use crate::utils::fmgr::FunctionCallInfo;
use crate::utils::palloc::pstrdup;

extern "C" {
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
}

// ---------------------------------------------------------------------------
// Stubs for symbols not yet ported.  These functions live in other PostgreSQL
// modules that PepperDB has not translated yet; minimal local declarations keep
// this file's translation faithful and self-contained.
// ---------------------------------------------------------------------------

// TODO(pg-port): real transformExpr lives in parser/parse_expr.c.
unsafe fn transformExpr(pstate: *mut ParseState, expr: *mut Node, exprKind: ParseExprKind) -> *mut Node {
    let _ = (pstate, expr, exprKind);
    null_mut()
}

// TODO(pg-port): real addRangeTableEntryForTableFunc lives in parser/parse_relation.c.
unsafe fn addRangeTableEntryForTableFunc(
    pstate: *mut ParseState,
    tf: *mut TableFunc,
    alias: *mut c_void,
    lateral: bool,
    inFromCl: bool,
) -> *mut ParseNamespaceItem {
    let _ = (pstate, tf, alias, lateral, inFromCl);
    null_mut()
}

// TODO(pg-port): real get_typtype/type_is_array/getBaseType live in utils/cache/lsyscache.c.
unsafe fn get_typtype(typid: Oid) -> c_char {
    let _ = typid;
    0
}
unsafe fn type_is_array(typid: Oid) -> bool {
    let _ = typid;
    false
}
unsafe fn getBaseType(typid: Oid) -> Oid {
    typid
}

// TODO(pg-port): real jsonpath_in lives in utils/adt/jsonpath.c.
unsafe fn jsonpath_in(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    0
}

// errcode markers (ereport takes only level+msg per port rules).
// TODO(pg-port): real error codes live in utils/errcodes.h (generated).
const ERRCODE_SYNTAX_ERROR: c_int = 0;
const ERRCODE_DUPLICATE_ALIAS: c_int = 0;
unsafe fn errcode(sqlerrcode: c_int) -> c_int {
    sqlerrcode
}
// TODO(pg-port): real parser_errposition lives in parser/parse_node.c.
unsafe fn parser_errposition(pstate: *mut ParseState, location: c_int) -> c_int {
    let _ = (pstate, location);
    0
}

/* Context for transformJsonTableColumns() */
#[repr(C)]
pub struct JsonTableParseContext {
    pub pstate: *mut ParseState,
    pub jt: *mut JsonTable,
    pub tf: *mut TableFunc,
    pub pathNames: *mut List, /* list of all path and columns names */
    pub pathNameId: c_int,    /* path name id counter */
}

/*
 * transformJsonTable -
 *			Transform a raw JsonTable into TableFunc
 *
 * Mainly, this transforms the JSON_TABLE() document-generating expression
 * (jt->context_item) and the column-generating expressions (jt->columns) to
 * populate TableFunc.docexpr and TableFunc.colvalexprs, respectively. Also,
 * the PASSING values (jt->passing) are transformed and added into
 * TableFunc.passingvalexprs.
 */
pub unsafe fn transformJsonTable(
    pstate: *mut ParseState,
    jt: *mut JsonTable,
) -> *mut ParseNamespaceItem {
    let tf: *mut TableFunc;
    let jfe: *mut JsonFuncExpr;
    let je: *mut JsonExpr;
    let rootPathSpec: *mut JsonTablePathSpec = (*jt).pathspec;
    let is_lateral: bool;
    let mut cxt: JsonTableParseContext = JsonTableParseContext {
        pstate,
        jt: null_mut(),
        tf: null_mut(),
        pathNames: null_mut(),
        pathNameId: 0,
    };

    Assert!(
        IsA!((*rootPathSpec).string, T_A_Const)
            && IsA!(&mut (*castNode!(A_Const, T_A_Const, (*rootPathSpec).string)).val, T_String)
    );

    if !(*jt).on_error.is_null()
        && (*(*jt).on_error).btype != JSON_BEHAVIOR_ERROR
        && (*(*jt).on_error).btype != JSON_BEHAVIOR_EMPTY
        && (*(*jt).on_error).btype != JSON_BEHAVIOR_EMPTY_ARRAY
    {
        let _ = errcode(ERRCODE_SYNTAX_ERROR);
        let _ = parser_errposition(pstate, (*(*jt).on_error).location);
        ereport!(ERROR, errmsg!("invalid {} behavior", "ON ERROR"));
    }

    cxt.pathNameId = 0;
    if (*rootPathSpec).name.is_null() {
        (*rootPathSpec).name = generateJsonTablePathName(&raw mut cxt);
    }
    cxt.pathNames = list_make1!((*rootPathSpec).name as *mut c_void);
    CheckDuplicateColumnOrPathNames(&raw mut cxt, (*jt).columns);

    /*
     * We make lateral_only names of this level visible, whether or not the
     * RangeTableFunc is explicitly marked LATERAL.  This is needed for SQL
     * spec compliance and seems useful on convenience grounds for all
     * functions in FROM.
     *
     * (LATERAL can't nest within a single pstate level, so we don't need
     * save/restore logic here.)
     */
    Assert!(!(*pstate).p_lateral_active);
    (*pstate).p_lateral_active = true;

    tf = makeNode!(TableFunc, T_TableFunc);
    (*tf).functype = TFT_JSON_TABLE;

    /*
     * Transform JsonFuncExpr representing the top JSON_TABLE context_item and
     * pathspec into a dummy JSON_TABLE_OP JsonExpr.
     */
    jfe = makeNode!(JsonFuncExpr, T_JsonFuncExpr);
    (*jfe).op = JSON_TABLE_OP;
    (*jfe).context_item = (*jt).context_item;
    (*jfe).pathspec = (*rootPathSpec).string;
    (*jfe).passing = (*jt).passing;
    (*jfe).on_empty = null_mut();
    (*jfe).on_error = (*jt).on_error;
    (*jfe).location = (*jt).location;
    (*tf).docexpr = transformExpr(pstate, jfe as *mut Node, EXPR_KIND_FROM_FUNCTION);

    /*
     * Create a JsonTablePlan that will generate row pattern that becomes
     * source data for JSON path expressions in jt->columns.  This also adds
     * the columns' transformed JsonExpr nodes into tf->colvalexprs.
     */
    cxt.jt = jt;
    cxt.tf = tf;
    (*tf).plan = transformJsonTableColumns(
        &raw mut cxt,
        (*jt).columns,
        (*jt).passing,
        rootPathSpec,
    ) as *mut Node;

    /*
     * Copy the transformed PASSING arguments into the TableFunc node, because
     * they are evaluated separately from the JsonExpr that we just put in
     * TableFunc.docexpr.  JsonExpr.passing_values is still kept around for
     * get_json_table().
     */
    je = (*tf).docexpr as *mut JsonExpr;
    (*tf).passingvalexprs = copyObjectImpl((*je).passing_values as *const c_void) as *mut List;

    (*tf).ordinalitycol = -1; /* undefine ordinality column number */
    (*tf).location = (*jt).location;

    (*pstate).p_lateral_active = false;

    /*
     * Mark the RTE as LATERAL if the user said LATERAL explicitly, or if
     * there are any lateral cross-references in it.
     */
    is_lateral = (*jt).lateral || contain_vars_of_level(tf as *mut Node, 0);

    addRangeTableEntryForTableFunc(pstate, tf, (*jt).alias as *mut c_void, is_lateral, true)
}

/*
 * Check if a column / path name is duplicated in the given shared list of
 * names.
 */
unsafe fn CheckDuplicateColumnOrPathNames(cxt: *mut JsonTableParseContext, columns: *mut List) {
    foreach!(lc1, columns, {
        let jtc: *mut JsonTableColumn =
            castNode!(JsonTableColumn, T_JsonTableColumn, lfirst(current_cell!(lc1)));

        if (*jtc).coltype == JTC_NESTED {
            if !(*(*jtc).pathspec).name.is_null() {
                if LookupPathOrColumnName(cxt, (*(*jtc).pathspec).name) {
                    let _ = errcode(ERRCODE_DUPLICATE_ALIAS);
                    let _ = parser_errposition((*cxt).pstate, (*(*jtc).pathspec).name_location);
                    ereport!(
                        ERROR,
                        errmsg!(
                            "duplicate JSON_TABLE column or path name: {}",
                            std::ffi::CStr::from_ptr((*(*jtc).pathspec).name).to_string_lossy()
                        )
                    );
                }
                (*cxt).pathNames =
                    lappend((*cxt).pathNames, (*(*jtc).pathspec).name as *mut c_void);
            }

            CheckDuplicateColumnOrPathNames(cxt, (*jtc).columns);
        } else {
            if LookupPathOrColumnName(cxt, (*jtc).name) {
                let _ = errcode(ERRCODE_DUPLICATE_ALIAS);
                let _ = parser_errposition((*cxt).pstate, (*jtc).location);
                ereport!(
                    ERROR,
                    errmsg!(
                        "duplicate JSON_TABLE column or path name: {}",
                        std::ffi::CStr::from_ptr((*jtc).name).to_string_lossy()
                    )
                );
            }
            (*cxt).pathNames = lappend((*cxt).pathNames, (*jtc).name as *mut c_void);
        }
    });
}

/*
 * Lookup a column/path name in the given name list, returning true if already
 * there.
 */
unsafe fn LookupPathOrColumnName(cxt: *mut JsonTableParseContext, name: *mut c_char) -> bool {
    foreach!(lc, (*cxt).pathNames, {
        if strcmp(name, lfirst(current_cell!(lc)) as *const c_char) == 0 {
            return true;
        }
    });

    false
}

/* Generate a new unique JSON_TABLE path name. */
unsafe fn generateJsonTablePathName(cxt: *mut JsonTableParseContext) -> *mut c_char {
    let mut namebuf: [c_char; 32] = [0; 32];
    let mut name: *mut c_char = namebuf.as_mut_ptr();

    snprintf_json_table_path(
        namebuf.as_mut_ptr(),
        core::mem::size_of::<[c_char; 32]>(),
        (*cxt).pathNameId,
    );
    (*cxt).pathNameId += 1;

    name = pstrdup(name);
    (*cxt).pathNames = lappend((*cxt).pathNames, name as *mut c_void);

    name
}

// Helper mirroring snprintf(namebuf, sizeof, "json_table_path_%d", id).
unsafe fn snprintf_json_table_path(buf: *mut c_char, size: usize, id: c_int) {
    let s = format!("json_table_path_{}\0", id);
    let bytes = s.as_bytes();
    let n = core::cmp::min(bytes.len(), size);
    core::ptr::copy_nonoverlapping(bytes.as_ptr() as *const c_char, buf, n);
    if size > 0 {
        *buf.add(core::cmp::min(n, size - 1)) = 0;
    }
}

/*
 * Create a JsonTablePlan that will supply the source row for 'columns'
 * using 'pathspec' and append the columns' transformed JsonExpr nodes and
 * their type/collation information to cxt->tf.
 */
unsafe fn transformJsonTableColumns(
    cxt: *mut JsonTableParseContext,
    columns: *mut List,
    passingArgs: *mut List,
    pathspec: *mut JsonTablePathSpec,
) -> *mut JsonTablePlan {
    let pstate: *mut ParseState = (*cxt).pstate;
    let jt: *mut JsonTable = (*cxt).jt;
    let tf: *mut TableFunc = (*cxt).tf;
    let mut ordinality_found: bool = false;
    let errorOnError: bool =
        !(*jt).on_error.is_null() && (*(*jt).on_error).btype == JSON_BEHAVIOR_ERROR;
    let contextItemTypid: Oid = exprType((*tf).docexpr);
    let mut colMin: c_int;
    let colMax: c_int;
    let childplan: *mut JsonTablePlan;

    /* Start of column range */
    colMin = list_length((*tf).colvalexprs);

    foreach!(col, columns, {
        let rawc: *mut JsonTableColumn =
            castNode!(JsonTableColumn, T_JsonTableColumn, lfirst(current_cell!(col)));
        let mut typid: Oid = 0;
        let mut typmod: int32 = -1;
        let mut typcoll: Oid = InvalidOid;
        let colexpr: *mut Node;

        if (*rawc).coltype != JTC_NESTED {
            Assert!(!(*rawc).name.is_null());
            (*tf).colnames =
                lappend((*tf).colnames, makeString(pstrdup((*rawc).name)) as *mut c_void);
        }

        /*
         * Determine the type and typmod for the new column. FOR ORDINALITY
         * columns are INTEGER by standard; the others are user-specified.
         */
        match (*rawc).coltype {
            JTC_FOR_ORDINALITY => {
                if ordinality_found {
                    let _ = errcode(ERRCODE_SYNTAX_ERROR);
                    let _ = parser_errposition(pstate, (*rawc).location);
                    ereport!(ERROR, errmsg!("only one FOR ORDINALITY column is allowed"));
                }
                ordinality_found = true;
                colexpr = null_mut();
                typid = INT4OID;
                typmod = -1;
            }

            JTC_REGULAR => {
                typenameTypeIdAndMod(pstate, (*rawc).typeName, &raw mut typid, &raw mut typmod);

                /*
                 * Use JTC_FORMATTED so as to use JSON_QUERY for this column
                 * if the specified type is one that's better handled using
                 * JSON_QUERY() or if non-default WRAPPER or QUOTES behavior
                 * is specified.
                 */
                if isCompositeType(typid)
                    || (*rawc).quotes != JS_QUOTES_UNSPEC
                    || (*rawc).wrapper != JSW_UNSPEC
                {
                    (*rawc).coltype = JTC_FORMATTED;
                }

                /* FALLTHROUGH */
                let jfe: *mut JsonFuncExpr;
                let param: *mut CaseTestExpr = makeNode!(CaseTestExpr, T_CaseTestExpr);

                (*param).collation = InvalidOid;
                (*param).typeId = contextItemTypid;
                (*param).typeMod = -1;

                jfe = transformJsonTableColumn(rawc, param as *mut Node, passingArgs);

                colexpr = transformExpr(pstate, jfe as *mut Node, EXPR_KIND_FROM_FUNCTION);
                assign_expr_collations(pstate, colexpr);

                typid = exprType(colexpr);
                typmod = exprTypmod(colexpr);
                typcoll = exprCollation(colexpr);
            }

            JTC_FORMATTED | JTC_EXISTS => {
                let jfe: *mut JsonFuncExpr;
                let param: *mut CaseTestExpr = makeNode!(CaseTestExpr, T_CaseTestExpr);

                (*param).collation = InvalidOid;
                (*param).typeId = contextItemTypid;
                (*param).typeMod = -1;

                jfe = transformJsonTableColumn(rawc, param as *mut Node, passingArgs);

                colexpr = transformExpr(pstate, jfe as *mut Node, EXPR_KIND_FROM_FUNCTION);
                assign_expr_collations(pstate, colexpr);

                typid = exprType(colexpr);
                typmod = exprTypmod(colexpr);
                typcoll = exprCollation(colexpr);
            }

            JTC_NESTED => {
                continue;
            }
        }

        (*tf).coltypes = lappend_oid((*tf).coltypes, typid);
        (*tf).coltypmods = lappend_int((*tf).coltypmods, typmod);
        (*tf).colcollations = lappend_oid((*tf).colcollations, typcoll);
        (*tf).colvalexprs = lappend((*tf).colvalexprs, colexpr as *mut c_void);
    });

    /* End of column range. */
    if list_length((*tf).colvalexprs) == colMin {
        /* No columns in this Scan beside the nested ones. */
        colMax = -1;
        colMin = -1;
    } else {
        colMax = list_length((*tf).colvalexprs) - 1;
    }

    /* Recursively transform nested columns */
    childplan = transformJsonTableNestedColumns(cxt, passingArgs, columns);

    /* Create a "parent" scan responsible for all columns handled above. */
    makeJsonTablePathScan(pathspec, errorOnError, colMin, colMax, childplan)
}

/*
 * Check if the type is "composite" for the purpose of checking whether to use
 * JSON_VALUE() or JSON_QUERY() for a given JsonTableColumn.
 */
unsafe fn isCompositeType(typid: Oid) -> bool {
    let typtype: c_char = get_typtype(typid);

    typid == JSONOID
        || typid == JSONBOID
        || typid == RECORDOID
        || type_is_array(typid)
        || typtype == TYPTYPE_COMPOSITE
        /* domain over one of the above? */
        || (typtype == TYPTYPE_DOMAIN && isCompositeType(getBaseType(typid)))
}

/*
 * Transform JSON_TABLE column definition into a JsonFuncExpr
 * This turns:
 *   - regular column into JSON_VALUE()
 *   - FORMAT JSON column into JSON_QUERY()
 *   - EXISTS column into JSON_EXISTS()
 */
unsafe fn transformJsonTableColumn(
    jtc: *mut JsonTableColumn,
    contextItemExpr: *mut Node,
    passingArgs: *mut List,
) -> *mut JsonFuncExpr {
    let pathspec: *mut Node;
    let jfexpr: *mut JsonFuncExpr = makeNode!(JsonFuncExpr, T_JsonFuncExpr);

    if (*jtc).coltype == JTC_REGULAR {
        (*jfexpr).op = JSON_VALUE_OP;
    } else if (*jtc).coltype == JTC_EXISTS {
        (*jfexpr).op = JSON_EXISTS_OP;
    } else {
        (*jfexpr).op = JSON_QUERY_OP;
    }

    /* Pass the column name so any runtime JsonExpr errors can print it. */
    Assert!(!(*jtc).name.is_null());
    (*jfexpr).column_name = pstrdup((*jtc).name);

    (*jfexpr).context_item = makeJsonValueExpr(
        contextItemExpr as *mut Expr,
        null_mut(),
        makeJsonFormat(JS_FORMAT_DEFAULT, JS_ENC_DEFAULT, -1),
    );
    if !(*jtc).pathspec.is_null() {
        pathspec = (*(*jtc).pathspec).string;
    } else {
        /* Construct default path as '$."column_name"' */
        let mut path: StringInfoData = core::mem::zeroed();

        initStringInfo(&raw mut path);

        appendStringInfoString(&raw mut path, c"$.".as_ptr());
        escape_json(&raw mut path, (*jtc).name);

        pathspec = makeStringConst(path.data, -1);
    }
    (*jfexpr).pathspec = pathspec;
    (*jfexpr).passing = passingArgs;
    (*jfexpr).output = makeNode!(JsonOutput, T_JsonOutput);
    (*(*jfexpr).output).typeName = (*jtc).typeName;
    (*(*jfexpr).output).returning = makeNode!(JsonReturning, T_JsonReturning);
    (*(*(*jfexpr).output).returning).format = (*jtc).format;
    (*jfexpr).on_empty = (*jtc).on_empty;
    (*jfexpr).on_error = (*jtc).on_error;
    (*jfexpr).quotes = (*jtc).quotes;
    (*jfexpr).wrapper = (*jtc).wrapper;
    (*jfexpr).location = (*jtc).location;

    jfexpr
}

/*
 * Recursively transform nested columns and create child plan(s) that will be
 * used to evaluate their row patterns.
 */
unsafe fn transformJsonTableNestedColumns(
    cxt: *mut JsonTableParseContext,
    passingArgs: *mut List,
    columns: *mut List,
) -> *mut JsonTablePlan {
    let mut plan: *mut JsonTablePlan = null_mut();

    /*
     * If there are multiple NESTED COLUMNS clauses in 'columns', their
     * respective plans will be combined using a "sibling join" plan, which
     * effectively does a UNION of the sets of rows coming from each nested
     * plan.
     */
    foreach!(lc, columns, {
        let jtc: *mut JsonTableColumn =
            castNode!(JsonTableColumn, T_JsonTableColumn, lfirst(current_cell!(lc)));
        let nested: *mut JsonTablePlan;

        if (*jtc).coltype != JTC_NESTED {
            continue;
        }

        if (*(*jtc).pathspec).name.is_null() {
            (*(*jtc).pathspec).name = generateJsonTablePathName(cxt);
        }

        nested = transformJsonTableColumns(cxt, (*jtc).columns, passingArgs, (*jtc).pathspec);

        if !plan.is_null() {
            plan = makeJsonTableSiblingJoin(plan, nested);
        } else {
            plan = nested;
        }
    });

    plan
}

/*
 * Create a JsonTablePlan for given path and ON ERROR behavior.
 *
 * colMin and colMin give the range of columns computed by this scan in the
 * global flat list of column expressions that will be passed to the
 * JSON_TABLE's TableFunc.  Both are -1 when all of columns are nested and
 * thus computed by 'childplan'.
 */
unsafe fn makeJsonTablePathScan(
    pathspec: *mut JsonTablePathSpec,
    errorOnError: bool,
    colMin: c_int,
    colMax: c_int,
    childplan: *mut JsonTablePlan,
) -> *mut JsonTablePlan {
    let scan: *mut JsonTablePathScan = makeNode!(JsonTablePathScan, T_JsonTablePathScan);
    let pathstring: *mut c_char;
    let value: *mut Const;

    Assert!(IsA!((*pathspec).string, T_A_Const));
    pathstring = (&(*castNode!(A_Const, T_A_Const, (*pathspec).string)).val.sval).sval;
    value = makeConst(
        JSONPATHOID,
        -1,
        InvalidOid,
        -1,
        DirectFunctionCall1!(jsonpath_in as crate::utils::fmgr::PGFunction, CStringGetDatum(pathstring)),
        false,
        false,
    );

    (*scan).plan.r#type = NodeTag::T_JsonTablePathScan;
    (*scan).path = makeJsonTablePath(value, (*pathspec).name);
    (*scan).errorOnError = errorOnError;

    (*scan).child = childplan;

    (*scan).colMin = colMin;
    (*scan).colMax = colMax;

    scan as *mut JsonTablePlan
}

/*
 * Create a JsonTablePlan that will perform a join of the rows coming from
 * 'lplan' and 'rplan'.
 *
 * The default way of "joining" the rows is to perform a UNION between the
 * sets of rows from 'lplan' and 'rplan'.
 */
unsafe fn makeJsonTableSiblingJoin(
    lplan: *mut JsonTablePlan,
    rplan: *mut JsonTablePlan,
) -> *mut JsonTablePlan {
    let join: *mut JsonTableSiblingJoin = makeNode!(JsonTableSiblingJoin, T_JsonTableSiblingJoin);

    (*join).plan.r#type = NodeTag::T_JsonTableSiblingJoin;
    (*join).lplan = lplan;
    (*join).rplan = rplan;

    join as *mut JsonTablePlan
}
