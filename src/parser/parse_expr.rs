/*-------------------------------------------------------------------------
 *
 * parse_expr.rs
 *   handle expressions in parser
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *   src/backend/parser/parse_expr.c
 *
 *-------------------------------------------------------------------------
 */

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void};

use crate::c::OidIsValid;
use crate::postgres_ext::Oid;
use crate::postgres::Datum;
use crate::c::int32;

use crate::nodes::nodes::{nodeTag, Node, NodeTag, NodeTag::*};
use crate::nodes::pg_list::{
    List, NIL,
    lfirst,
    linitial, lsecond, lthird, lfourth, llast,
    lappend, lappend_oid, lcons, list_concat, list_length, list_make1_impl,
    list_nth, list_truncate, list_delete_last,
};
use crate::nodes::bitmapset::{Bitmapset, bms_add_member, bms_int_members, bms_next_member};
use crate::nodes::nodeFuncs::{
    exprType, exprTypmod, exprCollation, exprLocation,
    expression_returns_set, expression_tree_walker,
};
// copyObjectImpl: copyfuncs module not yet enabled; stub below
#[allow(dead_code)]
unsafe fn copyObjectImpl(from: *const core::ffi::c_void) -> *mut core::ffi::c_void {
    unimplemented!("copyObjectImpl not yet translated")
}
use crate::nodes::makefuncs::{
    makeConst, makeBoolConst, makeBoolExpr, makeRangeVar, makeTargetEntry,
    makeSimpleA_Expr, makeJsonFormat, makeJsonValueExpr, makeJsonBehavior,
    makeJsonIsPredicate, makeFuncExpr,
};
use crate::nodes::value::{makeString};
use crate::nodes::primnodes::{
    Expr, Var, Param, ParamKind, PARAM_SUBLINK, PARAM_MULTIEXPR, PARAM_EXTERN,
    CaseTestExpr, NullTest, NullTestType, BooleanTest, BoolTestType,
    OpExpr, RowCompareExpr, CoalesceExpr, MinMaxExpr, ArrayExpr,
    RowExpr, CollateExpr, SubLink, SubLinkType,
    SQLValueFunction, SQLValueFunctionOp,
    XmlExpr, XmlExprOp,
    FuncExpr, CoercionForm,
    JsonConstructorExpr, JsonConstructorType,
    JsonExpr, JsonBehavior, JsonBehaviorType,
    JsonReturning, JsonValueExpr, CompareType,
    WindowFunc,
};
use crate::nodes::parsenodes::{
    A_Const, A_Expr, A_Expr_Kind as A_ExprKind, A_Indirection, A_ArrayExpr, A_Star,
    A_Indices, TypeCast, CollateClause, ColumnRef, ParamRef,
    FuncCall, MultiAssignRef,
    XmlSerialize,
    JsonObjectConstructor, JsonArrayConstructor, JsonArrayQueryConstructor,
    JsonObjectAgg, JsonArrayAgg, JsonAggConstructor,
    JsonParseExpr, JsonScalarExpr, JsonSerializeExpr, JsonFuncExpr,
    JsonOutput,
    JsonKeyValue, JsonArgument,
    ResTarget, SortBy, SelectStmt, RangeSubselect,
    Query,
};
use crate::nodes::primnodes::{
    Aggref, TargetEntry, Alias,
    BoolExpr, BoolExprType,
    GroupingFunc, MergeSupportFunc,
    NamedArgExpr,
    CaseExpr, CaseWhen,
    JsonIsPredicate, JsonValueExpr as ParseJsonValueExpr, JsonFormat,
    RowExpr as ParseRowExpr,
    CoalesceExpr as ParseCoalesceExpr, MinMaxExpr as ParseMinMaxExpr,
    NullTest as ParseNullTest, BooleanTest as ParseBooleanTest,
    CurrentOfExpr, SetToDefault,
    XmlExpr as ParseXmlExpr,
};
use crate::nodes::nodes::{AggSplit};
use crate::catalog::pg_aggregate::{AGGKIND_NORMAL};
use crate::parser::parse_node::ParseNamespaceItem;
/* ExecAggref is the same as Aggref from primnodes */
use crate::nodes::primnodes::Aggref as ExecAggref;

use crate::parser::parse_node::{
    ParseState, ParseExprKind, ParseExprKind::*,
    make_parsestate, free_parsestate,
    parser_errposition, transformContainerSubscripts,
};
use crate::parser::parse_collate::assign_expr_collations;
use crate::parser::parse_oper::{make_op, make_scalar_array_op};
use crate::parser::parse_type::{
    typenameTypeIdAndMod, LookupCollation,
};
use crate::ISCOMPLEX;

use crate::utils::cache::lsyscache::{
    format_type_be,
    get_array_type, get_element_type,
    getBaseType, getBaseTypeAndTypmod,
    get_typtype, get_typcollation, type_is_collatable, type_is_rowtype,
    get_type_category_preferred,
    get_op_index_interpretation, get_collation_name,
    BoolGetDatum, CStringGetDatum, pstrdup, palloc,
};
use crate::utils::cache::typcache::DomainHasConstraints;

use crate::miscadmin::check_stack_depth;

/* TODO(pg-port): unported parser siblings - stubs */
/* parse_agg.c */
unsafe fn transformAggregateCall(
    _pstate: *mut ParseState, _aggref: *mut Aggref,
    _args: *mut List, _agg_order: *mut List, _expand_star: bool,
) { todo!("transformAggregateCall") }
unsafe fn transformGroupingFunc(
    _pstate: *mut ParseState, _gf: *mut GroupingFunc,
) -> *mut Node { todo!("transformGroupingFunc") }
unsafe fn transformWindowFuncCall(
    _pstate: *mut ParseState, _wfunc: *mut WindowFunc, _over: *mut c_void,
) { todo!("transformWindowFuncCall") }
/* parse_func.c */
unsafe fn ParseFuncOrColumn(
    _pstate: *mut ParseState, _funcname: *mut List, _fargs: *mut List,
    _last_srf: *mut Node, _fn_: *mut FuncCall, _proc_call: bool,
    _location: c_int,
) -> *mut Node { todo!("ParseFuncOrColumn") }
/* parse_clause.c */
unsafe fn transformWhereClause(
    _pstate: *mut ParseState, _clause: *mut Node,
    _exprKind: ParseExprKind, _constructName: *const c_char,
) -> *mut Node { todo!("transformWhereClause") }
/* analyze.c */
unsafe fn parse_sub_analyze(
    _parseTree: *mut Node, _parentParseState: *mut ParseState,
    _queryEnv: *mut c_void, _locked_from_parent: bool,
    _resolve_unknowns: bool,
) -> *mut Query { todo!("parse_sub_analyze") }
unsafe fn transformStmt(
    _pstate: *mut ParseState, _parseTree: *mut Node,
) -> *mut Query { todo!("transformStmt") }
/* parse_relation.c */
unsafe fn colNameToVar(
    _pstate: *mut ParseState, _colname: *const c_char, _localonly: bool,
    _location: c_int,
) -> *mut Node { todo!("colNameToVar") }
unsafe fn scanNSItemForColumn(
    _pstate: *mut ParseState, _nsitem: *mut ParseNamespaceItem,
    _sublevels_up: c_int, _colname: *const c_char, _location: c_int,
) -> *mut Node { todo!("scanNSItemForColumn") }
unsafe fn refnameNamespaceItem(
    _pstate: *mut ParseState, _schemaname: *const c_char,
    _refname: *const c_char, _location: c_int, _levels_up: *mut c_int,
) -> *mut ParseNamespaceItem { todo!("refnameNamespaceItem") }
unsafe fn GetRTEByRangeTablePosn(
    _pstate: *mut ParseState, _varno: c_int, _sublevels_up: c_int,
) -> *mut crate::nodes::parsenodes::RangeTblEntry { todo!("GetRTEByRangeTablePosn") }
unsafe fn errorMissingColumn(
    _pstate: *mut ParseState, _relname: *const c_char,
    _colname: *const c_char, _location: c_int,
) { todo!("errorMissingColumn") }
unsafe fn errorMissingRTE(
    _pstate: *mut ParseState, _relation: *mut crate::nodes::primnodes::RangeVar,
) { todo!("errorMissingRTE") }
unsafe fn expandRTE(
    _rte: *mut crate::nodes::parsenodes::RangeTblEntry, _rtindex: c_int,
    _sublevels_up: c_int, _returning_type: c_int, _location: c_int,
    _include_dropped: bool, _colnames: *mut *mut List, _colvars: *mut *mut List,
) { todo!("expandRTE") }
unsafe fn markNullableIfNeeded(_pstate: *mut ParseState, _var: *mut Var) {
    todo!("markNullableIfNeeded")
}
unsafe fn markVarForSelectPriv(_pstate: *mut ParseState, _var: *mut Var) {
    todo!("markVarForSelectPriv")
}
unsafe fn makeWholeRowVar(
    _rte: *mut crate::nodes::parsenodes::RangeTblEntry, _rtindex: c_int,
    _sublevels_up: c_int, _allowScalar: bool,
) -> *mut Var { todo!("makeWholeRowVar") }
/* parse_target.c */
unsafe fn FigureColname(_node: *mut Node) -> *mut c_char { todo!("FigureColname") }
unsafe fn transformExpressionList(
    _pstate: *mut ParseState, _exprlist: *mut List,
    _exprKind: ParseExprKind, _allowDefault: bool,
) -> *mut List { todo!("transformExpressionList") }
/* parse_coerce.c */
unsafe fn coerce_to_boolean(
    _pstate: *mut ParseState, _node: *mut Node, _constructName: *const c_char,
) -> *mut Node { todo!("coerce_to_boolean") }
unsafe fn coerce_to_common_type(
    _pstate: *mut ParseState, _node: *mut Node,
    _targetTypeId: Oid, _constructName: *const c_char,
) -> *mut Node { todo!("coerce_to_common_type") }
unsafe fn coerce_to_target_type(
    _pstate: *mut ParseState, _expr: *mut Node, _exprtype: Oid,
    _targettype: Oid, _targettypmod: int32,
    _ccontext: c_int, _cformat: c_int, _location: c_int,
) -> *mut Node { todo!("coerce_to_target_type") }
unsafe fn coerce_to_specific_type(
    _pstate: *mut ParseState, _node: *mut Node,
    _targetTypeId: Oid, _constructName: *const c_char,
) -> *mut Node { todo!("coerce_to_specific_type") }
unsafe fn select_common_type(
    _pstate: *mut ParseState, _exprs: *mut List,
    _context: *const c_char, _which_expr: *mut *mut Node,
) -> Oid { todo!("select_common_type") }
unsafe fn verify_common_type(_typid: Oid, _exprs: *mut List) -> bool {
    todo!("verify_common_type")
}
unsafe fn parser_coercion_errposition(
    _pstate: *mut ParseState, _coerce_location: c_int, _input_expr: *mut Node,
) -> c_int { todo!("parser_coercion_errposition") }
/* xml.c */
unsafe fn map_sql_identifier_to_xml_name(
    _ident: *mut c_char, _fully_escaped: bool, _escape_period: bool,
) -> *mut c_char { todo!("map_sql_identifier_to_xml_name") }
/* timestamp.c */
unsafe fn anytime_typmod_check(_istz: bool, _typmod: int32) -> int32 {
    todo!("anytime_typmod_check")
}
unsafe fn anytimestamp_typmod_check(_istz: bool, _typmod: int32) -> int32 {
    todo!("anytimestamp_typmod_check")
}
/* dbcommands.c */
unsafe fn get_database_name(_dboid: Oid) -> *mut c_char {
    todo!("get_database_name")
}
/* catalog/namespace.c */
unsafe fn NameListToString(_names: *const List) -> *mut c_char {
    todo!("NameListToString")
}
/* jsonb_in -- utils/adt/jsonb.c (not yet ported) */
mod jsonb_stub {
    use crate::postgres::Datum;
    pub unsafe fn jsonb_in(_fcinfo: crate::utils::fmgr::FunctionCallInfo) -> Datum {
        todo!("jsonb_in not yet ported")
    }
}

/* make_const -- in parse_node.c */
unsafe fn make_const(
    _pstate: *mut ParseState, _aconst: *mut A_Const,
) -> *mut crate::nodes::primnodes::Const { todo!("make_const") }
/* fmgroids */
const F_CONVERT_FROM: Oid = 0; // TODO(pg-port): real OID
const F_CONVERT_TO: Oid = 0;   // TODO(pg-port): real OID
const F_TO_JSON: Oid = 0;      // TODO(pg-port): real OID
const F_TO_JSONB: Oid = 0;     // TODO(pg-port): real OID
const F_JSONB_OBJECT_AGG_UNIQUE_STRICT: Oid = 0; // TODO(pg-port)
const F_JSONB_OBJECT_AGG_STRICT: Oid = 0;        // TODO(pg-port)
const F_JSONB_OBJECT_AGG_UNIQUE: Oid = 0;        // TODO(pg-port)
const F_JSONB_OBJECT_AGG: Oid = 0;               // TODO(pg-port)
const F_JSON_OBJECT_AGG_UNIQUE_STRICT: Oid = 0;  // TODO(pg-port)
const F_JSON_OBJECT_AGG_STRICT: Oid = 0;         // TODO(pg-port)
const F_JSON_OBJECT_AGG_UNIQUE: Oid = 0;         // TODO(pg-port)
const F_JSON_OBJECT_AGG: Oid = 0;                // TODO(pg-port)
const F_JSONB_AGG_STRICT: Oid = 0;               // TODO(pg-port)
const F_JSONB_AGG: Oid = 0;                      // TODO(pg-port)
const F_JSON_AGG_STRICT: Oid = 0;                // TODO(pg-port)
const F_JSON_AGG: Oid = 0;                       // TODO(pg-port)
/* catalog/pg_type_d.h OIDs */
use crate::catalog::pg_type_d::{
    BOOLOID, BYTEAOID, NAMEOID, TEXTOID, INT2OID, INT4OID, INT8OID,
    FLOAT4OID, FLOAT8OID, NUMERICOID, VARCHAROID,
    DATEOID, TIMEOID, TIMETZOID, TIMESTAMPOID, TIMESTAMPTZOID,
    XMLOID, JSONOID, JSONBOID, JSONPATHOID,
    REFCURSOROID, RECORDOID, INT2VECTOROID, OIDVECTOROID,
    UNKNOWNOID,
};
/* coercion constants -- from nodes/primnodes.h */
const COERCION_IMPLICIT: c_int = 0;
const COERCION_ASSIGNMENT: c_int = 1;
const COERCION_EXPLICIT: c_int = 2;
const COERCE_IMPLICIT_CAST: c_int = 0;
const COERCE_EXPLICIT_CAST: c_int = 1;
const COERCE_EXPLICIT_CALL: c_int = 2;
/* type categories from utils/lsyscache.h */
const TYPCATEGORY_STRING: c_char = 'S' as c_char;
const TYPCATEGORY_BITSTRING: c_char = 'V' as c_char;
/* typtype constants from catalog/pg_type.h */
const TYPTYPE_PSEUDO: c_char = 'p' as c_char;
const TYPTYPE_DOMAIN: c_char = 'd' as c_char;
/* misc */
const MaxTupleAttributeNumber: c_int = 1664;
const NAMEDATALEN: usize = 64;

/* GUC parameters */
pub static mut Transform_null_equals: bool = false;

/* C standard library functions used here */
extern "C" {
    fn strcmp(s1: *const c_char, s2: *const c_char) -> c_int;
}

/* Inline helpers wrapping unsafe foreign/local calls */
#[inline]
unsafe fn cstr_to_str(s: *const c_char) -> &'static str {
    if s.is_null() { return ""; }
    unsafe { std::ffi::CStr::from_ptr(s).to_str().unwrap_or("?") }
}

#[inline]
unsafe fn copyObject<T>(p: *const T) -> *mut T {
    copyObjectImpl(p as *const c_void) as *mut T
}

#[inline]
unsafe fn InvalidOid() -> Oid { 0 }

/* convenience alias matching C macro */
macro_rules! InvalidOid { () => { 0 as Oid } }

/* cstr!(literal) - produce a *const c_char from a string literal */
macro_rules! cstr {
    ($s:literal) => {
        ::core::concat!($s, "\0").as_ptr() as *const ::core::ffi::c_char
    };
}

/* errhint!, errdetail!, errmsg_internal! - stubs (info discarded by ereport! shim) */
macro_rules! errhint {
    ($($arg:tt)*) => { () };
}
macro_rules! errdetail {
    ($($arg:tt)*) => { () };
}
macro_rules! errmsg_internal {
    ($fmt:literal $(, $arg:expr)*) => {
        errmsg!($fmt $(, $arg)*)
    };
}

/* snprintf wrapper */
macro_rules! snprintf_buf {
    ($buf:expr, $fmt:expr, $($arg:expr),*) => {{
        let s = format!($fmt, $($arg),*);
        let bytes = s.as_bytes();
        let n = bytes.len().min($buf.len() - 1);
        $buf[..n].copy_from_slice(&bytes[..n]);
        $buf[n] = 0;
    }};
}

/* JSON op codes - from nodes/primnodes.h */
use crate::nodes::primnodes::JsonExprOp::{
    JSON_EXISTS_OP, JSON_QUERY_OP, JSON_VALUE_OP, JSON_TABLE_OP,
};
use crate::nodes::parsenodes::JsonQuotes::{JS_QUOTES_OMIT};
use crate::nodes::primnodes::JsonWrapper::{JSW_CONDITIONAL, JSW_UNCONDITIONAL};
use crate::nodes::primnodes::{JsonFormatType, JsonEncoding};
use crate::nodes::primnodes::JsonFormatType::*;
use crate::nodes::primnodes::JsonEncoding::*;
use crate::nodes::primnodes::JsonBehaviorType::*;
use crate::nodes::primnodes::JsonConstructorType::*;

use crate::nodes::primnodes::MinMaxOp::IS_GREATEST;
use crate::nodes::primnodes::XmlExprOp::*;

use crate::nodes::nodes::NodeTag::{T_Var, T_Const, T_FuncExpr, T_OpExpr,
    T_CoerceViaIO, T_CoerceToDomain, T_ArrayCoerceExpr,
    T_ConvertRowtypeExpr, T_RelabelType, T_CollateExpr, T_DistinctExpr,
    T_NullIfExpr, T_JsonAggConstructor};

use crate::{
    IsA, makeNode, castNode, NodeSetTag,
    foreach, forboth, current_cell, lfirst_node,
    linitial_node, lsecond_node, lthird_node, lfourth_node,
    list_make1, list_make2,
    strVal,
};

/*
 * transformExpr -
 *   Analyze and transform expressions. Type checking and type casting is
 *   done here.  This processing converts the raw grammar output into
 *   expression trees with fully determined semantics.
 */
pub unsafe fn transformExpr(
    pstate: *mut ParseState,
    expr: *mut Node,
    exprKind: ParseExprKind,
) -> *mut Node {
    let result: *mut Node;
    let sv_expr_kind: ParseExprKind;

    /* Save and restore identity of expression type we're parsing */
    Assert!(exprKind != EXPR_KIND_NONE);
    sv_expr_kind = (*pstate).p_expr_kind;
    (*pstate).p_expr_kind = exprKind;

    result = transformExprRecurse(pstate, expr);

    (*pstate).p_expr_kind = sv_expr_kind;

    result
}

unsafe fn transformExprRecurse(
    pstate: *mut ParseState,
    expr: *mut Node,
) -> *mut Node {
    let mut result: *mut Node = std::ptr::null_mut();

    if expr.is_null() {
        return std::ptr::null_mut();
    }

    /* Guard against stack overflow due to overly complex expressions */
    check_stack_depth();

    match nodeTag(expr) {
        T_ColumnRef => {
            result = transformColumnRef(pstate, expr as *mut ColumnRef);
        }
        T_ParamRef => {
            result = transformParamRef(pstate, expr as *mut ParamRef);
        }
        T_A_Const => {
            result = make_const(pstate, expr as *mut A_Const) as *mut Node;
        }
        T_A_Indirection => {
            result = transformIndirection(pstate, expr as *mut A_Indirection);
        }
        T_A_ArrayExpr => {
            result = transformArrayExpr(
                pstate,
                expr as *mut A_ArrayExpr,
                InvalidOid!(),
                InvalidOid!(),
                -1,
            );
        }
        T_TypeCast => {
            result = transformTypeCast(pstate, expr as *mut TypeCast);
        }
        T_CollateClause => {
            result = transformCollateClause(pstate, expr as *mut CollateClause);
        }
        T_A_Expr => {
            let a = expr as *mut A_Expr;
            #[allow(unreachable_patterns)]
            match (*a).kind {
                A_ExprKind::AEXPR_OP => {
                    result = transformAExprOp(pstate, a);
                }
                A_ExprKind::AEXPR_OP_ANY => {
                    result = transformAExprOpAny(pstate, a);
                }
                A_ExprKind::AEXPR_OP_ALL => {
                    result = transformAExprOpAll(pstate, a);
                }
                A_ExprKind::AEXPR_DISTINCT | A_ExprKind::AEXPR_NOT_DISTINCT => {
                    result = transformAExprDistinct(pstate, a);
                }
                A_ExprKind::AEXPR_NULLIF => {
                    result = transformAExprNullIf(pstate, a);
                }
                A_ExprKind::AEXPR_IN => {
                    result = transformAExprIn(pstate, a);
                }
                A_ExprKind::AEXPR_LIKE
                | A_ExprKind::AEXPR_ILIKE
                | A_ExprKind::AEXPR_SIMILAR => {
                    /* we can transform these just like AEXPR_OP */
                    result = transformAExprOp(pstate, a);
                }
                A_ExprKind::AEXPR_BETWEEN
                | A_ExprKind::AEXPR_NOT_BETWEEN
                | A_ExprKind::AEXPR_BETWEEN_SYM
                | A_ExprKind::AEXPR_NOT_BETWEEN_SYM => {
                    result = transformAExprBetween(pstate, a);
                }
                _ => {
                    elog!(ERROR, "unrecognized A_Expr kind: {}", (*a).kind as c_int);
                    result = std::ptr::null_mut(); /* keep compiler quiet */
                }
            }
        }
        T_BoolExpr => {
            result = transformBoolExpr(pstate, expr as *mut BoolExpr);
        }
        T_FuncCall => {
            result = transformFuncCall(pstate, expr as *mut FuncCall);
        }
        T_MultiAssignRef => {
            result = transformMultiAssignRef(pstate, expr as *mut MultiAssignRef);
        }
        T_GroupingFunc => {
            result = transformGroupingFunc(pstate, expr as *mut GroupingFunc);
        }
        T_MergeSupportFunc => {
            result = transformMergeSupportFunc(pstate, expr as *mut MergeSupportFunc);
        }
        T_NamedArgExpr => {
            let na = expr as *mut NamedArgExpr;
            (*na).arg = transformExprRecurse(pstate, (*na).arg as *mut Node) as *mut Expr;
            result = expr;
        }
        T_SubLink => {
            result = transformSubLink(pstate, expr as *mut SubLink);
        }
        T_CaseExpr => {
            result = transformCaseExpr(pstate, expr as *mut CaseExpr);
        }
        T_RowExpr => {
            result = transformRowExpr(pstate, expr as *mut ParseRowExpr, false);
        }
        T_CoalesceExpr => {
            result = transformCoalesceExpr(pstate, expr as *mut ParseCoalesceExpr);
        }
        T_MinMaxExpr => {
            result = transformMinMaxExpr(pstate, expr as *mut ParseMinMaxExpr);
        }
        T_SQLValueFunction => {
            result = transformSQLValueFunction(pstate, expr as *mut SQLValueFunction);
        }
        T_XmlExpr => {
            result = transformXmlExpr(pstate, expr as *mut ParseXmlExpr);
        }
        T_XmlSerialize => {
            result = transformXmlSerialize(pstate, expr as *mut XmlSerialize);
        }
        T_NullTest => {
            let n = expr as *mut ParseNullTest;
            (*n).arg = transformExprRecurse(pstate, (*n).arg as *mut Node) as *mut Expr;
            /* the argument can be any type, so don't coerce it */
            (*n).argisrow = type_is_rowtype(exprType((*n).arg as *const Node));
            result = expr;
        }
        T_BooleanTest => {
            result = transformBooleanTest(pstate, expr as *mut ParseBooleanTest);
        }
        T_CurrentOfExpr => {
            result = transformCurrentOfExpr(pstate, expr as *mut CurrentOfExpr);
        }
        /*
         * In all places where DEFAULT is legal, the caller should have
         * processed it rather than passing it to transformExpr().
         */
        T_SetToDefault => {
            ereport!(
                ERROR,
                errmsg!("DEFAULT is not allowed in this context")
                /* C also: errcode, errdetail, errhint, parser_errposition */
            );
        }
        /*
         * CaseTestExpr doesn't require any processing; it is only
         * injected into parse trees in a fully-formed state.
         *
         * Ordinarily we should not see a Var here, but it is convenient
         * for transformJoinUsingClause() to create untransformed operator
         * trees containing already-transformed Vars.  The best
         * alternative would be to deconstruct and reconstruct column
         * references, which seems expensively pointless.  So allow it.
         */
        T_CaseTestExpr | T_Var => {
            result = expr;
        }
        T_JsonObjectConstructor => {
            result = transformJsonObjectConstructor(
                pstate,
                expr as *mut JsonObjectConstructor,
            );
        }
        T_JsonArrayConstructor => {
            result = transformJsonArrayConstructor(
                pstate,
                expr as *mut JsonArrayConstructor,
            );
        }
        T_JsonArrayQueryConstructor => {
            result = transformJsonArrayQueryConstructor(
                pstate,
                expr as *mut JsonArrayQueryConstructor,
            );
        }
        T_JsonObjectAgg => {
            result = transformJsonObjectAgg(pstate, expr as *mut JsonObjectAgg);
        }
        T_JsonArrayAgg => {
            result = transformJsonArrayAgg(pstate, expr as *mut JsonArrayAgg);
        }
        T_JsonIsPredicate => {
            result = transformJsonIsPredicate(pstate, expr as *mut JsonIsPredicate);
        }
        T_JsonParseExpr => {
            result = transformJsonParseExpr(pstate, expr as *mut JsonParseExpr);
        }
        T_JsonScalarExpr => {
            result = transformJsonScalarExpr(pstate, expr as *mut JsonScalarExpr);
        }
        T_JsonSerializeExpr => {
            result = transformJsonSerializeExpr(pstate, expr as *mut JsonSerializeExpr);
        }
        T_JsonFuncExpr => {
            result = transformJsonFuncExpr(pstate, expr as *mut JsonFuncExpr);
        }
        _ => {
            /* should not reach here */
            elog!(ERROR, "unrecognized node type: {}", nodeTag(expr) as c_int);
            result = std::ptr::null_mut(); /* keep compiler quiet */
        }
    }

    result
}

/*
 * helper routine for delivering "column does not exist" error message
 *
 * (Usually we don't have to work this hard, but the general case of field
 * selection from an arbitrary node needs it.)
 */
unsafe fn unknown_attribute(
    pstate: *mut ParseState,
    relref: *mut Node,
    attname: *const c_char,
    location: c_int,
) {
    let rte;

    if IsA!(relref, T_Var)
        && (*(relref as *mut Var)).varattno == 0 /* InvalidAttrNumber */
    {
        /* Reference the RTE by alias not by actual table name */
        rte = GetRTEByRangeTablePosn(
            pstate,
            (*(relref as *mut Var)).varno,
            (*(relref as *mut Var)).varlevelsup as c_int,
        );
        ereport!(
            ERROR,
            errmsg!("column {}.{} does not exist",
                     cstr_to_str((*(*rte).eref).aliasname), cstr_to_str(attname))
            /* C also: errcode, errdetail, errhint, parser_errposition */
        );
    } else {
        /* Have to do it by reference to the type of the expression */
        let rel_type_id = exprType(relref);

        if ISCOMPLEX!(rel_type_id) {
            ereport!(
                ERROR,
                errmsg!("column \"{}\" not found in data type {}",
                         cstr_to_str(attname), cstr_to_str(format_type_be(rel_type_id)))
                /* C also: errcode, errdetail, errhint, parser_errposition */
            );
        } else if rel_type_id == RECORDOID {
            ereport!(
                ERROR,
                errmsg!("could not identify column \"{}\" in record data type",
                         cstr_to_str(attname))
                /* C also: errcode, errdetail, errhint, parser_errposition */
            );
        } else {
            ereport!(
                ERROR,
                errmsg!("column notation .{} applied to type {}, \
                          which is not a composite type",
                         cstr_to_str(attname), cstr_to_str(format_type_be(rel_type_id)))
                /* C also: errcode, errdetail, errhint, parser_errposition */
            );
        }
    }
}

unsafe fn transformIndirection(
    pstate: *mut ParseState,
    ind: *mut A_Indirection,
) -> *mut Node {
    let last_srf = (*pstate).p_last_srf;
    let mut result = transformExprRecurse(pstate, (*ind).arg);
    let mut subscripts: *mut List = NIL;
    let location = exprLocation(result);

    /*
     * We have to split any field-selection operations apart from
     * subscripting.  Adjacent A_Indices nodes have to be treated as a single
     * multidimensional subscript operation.
     */
    foreach!(i_cell, (*ind).indirection, {
        let n = lfirst(current_cell!(i_cell)) as *mut Node;

        if IsA!(n, T_A_Indices) {
            subscripts = lappend(subscripts, n as *mut c_void);
        } else if IsA!(n, T_A_Star) {
            ereport!(
                ERROR,
                errmsg!("row expansion via \"*\" is not supported here")
                /* C also: errcode, errdetail, errhint, parser_errposition */
            );
        } else {
            let newresult: *mut Node;

            Assert!(IsA!(n, T_String));

            /* process subscripts before this field selection */
            if !subscripts.is_null() {
                result = transformContainerSubscripts(
                    pstate,
                    result,
                    exprType(result),
                    exprTypmod(result),
                    subscripts,
                    false,
                ) as *mut Node;
            }
            subscripts = NIL;

            newresult = ParseFuncOrColumn(
                pstate,
                list_make1!(n),
                list_make1!(result),
                last_srf,
                std::ptr::null_mut(),
                false,
                location,
            );
            if newresult.is_null() {
                unknown_attribute(pstate, result, strVal!(n), location);
            }
            result = newresult;
        }
    });

    /* process trailing subscripts, if any */
    if !subscripts.is_null() {
        result = transformContainerSubscripts(
            pstate,
            result,
            exprType(result),
            exprTypmod(result),
            subscripts,
            false,
        ) as *mut Node;
    }

    result
}

/*
 * Transform a ColumnRef.
 *
 * If you find yourself changing this code, see also ExpandColumnRefStar.
 */
unsafe fn transformColumnRef(
    pstate: *mut ParseState,
    cref: *mut ColumnRef,
) -> *mut Node {
    let mut node: *mut Node = std::ptr::null_mut();
    let mut nspname: *const c_char = std::ptr::null();
    let mut relname: *const c_char = std::ptr::null();
    let mut colname: *const c_char = std::ptr::null();
    let nsitem: *mut ParseNamespaceItem;
    let mut levels_up: c_int = 0;

    #[derive(PartialEq)]
    enum CrErr {
        NoColumn,
        NoRte,
        WrongDb,
        TooMany,
    }
    let mut crerr = CrErr::NoColumn;
    let mut err: *const c_char;

    /*
     * Check to see if the column reference is in an invalid place within the
     * query.  We allow column references in most places, except in default
     * expressions and partition bound expressions.
     */
    err = std::ptr::null();
    match (*pstate).p_expr_kind {
        EXPR_KIND_NONE => {
            Assert!(false); /* can't happen */
        }
        EXPR_KIND_OTHER
        | EXPR_KIND_JOIN_ON
        | EXPR_KIND_JOIN_USING
        | EXPR_KIND_FROM_SUBSELECT
        | EXPR_KIND_FROM_FUNCTION
        | EXPR_KIND_WHERE
        | EXPR_KIND_POLICY
        | EXPR_KIND_HAVING
        | EXPR_KIND_FILTER
        | EXPR_KIND_WINDOW_PARTITION
        | EXPR_KIND_WINDOW_ORDER
        | EXPR_KIND_WINDOW_FRAME_RANGE
        | EXPR_KIND_WINDOW_FRAME_ROWS
        | EXPR_KIND_WINDOW_FRAME_GROUPS
        | EXPR_KIND_SELECT_TARGET
        | EXPR_KIND_INSERT_TARGET
        | EXPR_KIND_UPDATE_SOURCE
        | EXPR_KIND_UPDATE_TARGET
        | EXPR_KIND_MERGE_WHEN
        | EXPR_KIND_GROUP_BY
        | EXPR_KIND_ORDER_BY
        | EXPR_KIND_DISTINCT_ON
        | EXPR_KIND_LIMIT
        | EXPR_KIND_OFFSET
        | EXPR_KIND_RETURNING
        | EXPR_KIND_MERGE_RETURNING
        | EXPR_KIND_VALUES
        | EXPR_KIND_VALUES_SINGLE
        | EXPR_KIND_CHECK_CONSTRAINT
        | EXPR_KIND_DOMAIN_CHECK
        | EXPR_KIND_FUNCTION_DEFAULT
        | EXPR_KIND_INDEX_EXPRESSION
        | EXPR_KIND_INDEX_PREDICATE
        | EXPR_KIND_STATS_EXPRESSION
        | EXPR_KIND_ALTER_COL_TRANSFORM
        | EXPR_KIND_EXECUTE_PARAMETER
        | EXPR_KIND_TRIGGER_WHEN
        | EXPR_KIND_PARTITION_EXPRESSION
        | EXPR_KIND_CALL_ARGUMENT
        | EXPR_KIND_COPY_WHERE
        | EXPR_KIND_GENERATED_COLUMN
        | EXPR_KIND_CYCLE_MARK => {
            /* okay */
        }
        EXPR_KIND_COLUMN_DEFAULT => {
            err = cstr!("cannot use column reference in DEFAULT expression");
        }
        EXPR_KIND_PARTITION_BOUND => {
            err = cstr!(
                "cannot use column reference in partition bound expression"
            );
        }
        /*
         * There is intentionally no default: case here, so that the
         * compiler will warn if we add a new ParseExprKind without
         * extending this switch.  If we do see an unrecognized value at
         * runtime, the behavior will be the same as for EXPR_KIND_OTHER,
         * which is sane anyway.
         */
        #[allow(unreachable_patterns)]
        _ => {}
    }
    if !err.is_null() {
        ereport!(
            ERROR,
            errmsg_internal!("{}", std::ffi::CStr::from_ptr(err).to_string_lossy())
            /* C also: errcode, errdetail, errhint, parser_errposition */
        );
    }

    /*
     * Give the PreParseColumnRefHook, if any, first shot.  If it returns
     * non-null then that's all, folks.
     */
    if let Some(hook) = (*pstate).p_pre_columnref_hook {
        node = hook(pstate, cref as *mut c_void);
        if !node.is_null() {
            return node;
        }
    }

    /*----------
     * The allowed syntaxes are:
     *
     * A         First try to resolve as unqualified column name;
     *           if no luck, try to resolve as unqualified table name (A.*).
     * A.B       A is an unqualified table name; B is either a
     *           column or function name (trying column name first).
     * A.B.C     schema A, table B, col or func name C.
     * A.B.C.D   catalog A, schema B, table C, col or func D.
     * A.*       A is an unqualified table name; means whole-row value.
     * A.B.*     whole-row value of table B in schema A.
     * A.B.C.*   whole-row value of table C in schema B in catalog A.
     *
     * We do not need to cope with bare "*"; that will only be accepted by
     * the grammar at the top level of a SELECT list, and transformTargetList
     * will take care of it before it ever gets here.  Also, "A.*" etc will
     * be expanded by transformTargetList if they appear at SELECT top level,
     * so here we are only going to see them as function or operator inputs.
     *
     * Currently, if a catalog name is given then it must equal the current
     * database name; we check it here and then discard it.
     *----------
     */
    match list_length((*cref).fields) {
        1 => {
            let field1 = linitial((*cref).fields) as *mut Node;

            colname = strVal!(field1);

            /* Try to identify as an unqualified column */
            node = colNameToVar(pstate, colname, false, (*cref).location);

            if node.is_null() {
                /*
                 * Not known as a column of any range-table entry.
                 *
                 * Try to find the name as a relation.  Note that only
                 * relations already entered into the rangetable will be
                 * recognized.
                 *
                 * This is a hack for backwards compatibility with
                 * PostQUEL-inspired syntax.  The preferred form now is
                 * "rel.*".
                 */
                nsitem = refnameNamespaceItem(
                    pstate,
                    std::ptr::null(),
                    colname,
                    (*cref).location,
                    &mut levels_up,
                );
                if !nsitem.is_null() {
                    node = transformWholeRowRef(
                        pstate,
                        nsitem,
                        levels_up,
                        (*cref).location,
                    );
                }
            }
        }
        2 => {
            let field1 = linitial((*cref).fields) as *mut Node;
            let field2 = lsecond((*cref).fields) as *mut Node;

            relname = strVal!(field1);

            /* Locate the referenced nsitem */
            nsitem = refnameNamespaceItem(
                pstate,
                nspname,
                relname,
                (*cref).location,
                &mut levels_up,
            );
            if nsitem.is_null() {
                crerr = CrErr::NoRte;
            } else {
                /* Whole-row reference? */
                if IsA!(field2, T_A_Star) {
                    node = transformWholeRowRef(
                        pstate, nsitem, levels_up, (*cref).location,
                    );
                } else {
                    colname = strVal!(field2);

                    /* Try to identify as a column of the nsitem */
                    node = scanNSItemForColumn(
                        pstate, nsitem, levels_up, colname, (*cref).location,
                    );
                    if node.is_null() {
                        /* Try it as a function call on the whole row */
                        node = transformWholeRowRef(
                            pstate, nsitem, levels_up, (*cref).location,
                        );
                        node = ParseFuncOrColumn(
                            pstate,
                            list_make1!(makeString(colname as *mut c_char)),
                            list_make1!(node),
                            (*pstate).p_last_srf,
                            std::ptr::null_mut(),
                            false,
                            (*cref).location,
                        );
                    }
                }
            }
        }
        3 => {
            let field1 = linitial((*cref).fields) as *mut Node;
            let field2 = lsecond((*cref).fields) as *mut Node;
            let field3 = lthird((*cref).fields) as *mut Node;

            nspname = strVal!(field1);
            relname = strVal!(field2);

            /* Locate the referenced nsitem */
            nsitem = refnameNamespaceItem(
                pstate,
                nspname,
                relname,
                (*cref).location,
                &mut levels_up,
            );
            if nsitem.is_null() {
                crerr = CrErr::NoRte;
            } else {
                /* Whole-row reference? */
                if IsA!(field3, T_A_Star) {
                    node = transformWholeRowRef(
                        pstate, nsitem, levels_up, (*cref).location,
                    );
                } else {
                    colname = strVal!(field3);

                    /* Try to identify as a column of the nsitem */
                    node = scanNSItemForColumn(
                        pstate, nsitem, levels_up, colname, (*cref).location,
                    );
                    if node.is_null() {
                        /* Try it as a function call on the whole row */
                        node = transformWholeRowRef(
                            pstate, nsitem, levels_up, (*cref).location,
                        );
                        node = ParseFuncOrColumn(
                            pstate,
                            list_make1!(makeString(colname as *mut c_char)),
                            list_make1!(node),
                            (*pstate).p_last_srf,
                            std::ptr::null_mut(),
                            false,
                            (*cref).location,
                        );
                    }
                }
            }
        }
        4 => {
            let field1 = linitial((*cref).fields) as *mut Node;
            let field2 = lsecond((*cref).fields) as *mut Node;
            let field3 = lthird((*cref).fields) as *mut Node;
            let field4 = lfourth((*cref).fields) as *mut Node;
            let catname: *const c_char;

            catname = strVal!(field1);
            nspname = strVal!(field2);
            relname = strVal!(field3);

            /*
             * We check the catalog name and then ignore it.
             */
            let db_name = get_database_name(crate::miscadmin::MyDatabaseId);
            if strcmp(catname, db_name) != 0 {
                crerr = CrErr::WrongDb;
            } else {
                /* Locate the referenced nsitem */
                nsitem = refnameNamespaceItem(
                    pstate,
                    nspname,
                    relname,
                    (*cref).location,
                    &mut levels_up,
                );
                if nsitem.is_null() {
                    crerr = CrErr::NoRte;
                } else {
                    /* Whole-row reference? */
                    if IsA!(field4, T_A_Star) {
                        node = transformWholeRowRef(
                            pstate, nsitem, levels_up, (*cref).location,
                        );
                    } else {
                        colname = strVal!(field4);

                        /* Try to identify as a column of the nsitem */
                        node = scanNSItemForColumn(
                            pstate, nsitem, levels_up, colname, (*cref).location,
                        );
                        if node.is_null() {
                            /* Try it as a function call on the whole row */
                            node = transformWholeRowRef(
                                pstate, nsitem, levels_up, (*cref).location,
                            );
                            node = ParseFuncOrColumn(
                                pstate,
                                list_make1!(makeString(colname as *mut c_char)),
                                list_make1!(node),
                                (*pstate).p_last_srf,
                                std::ptr::null_mut(),
                                false,
                                (*cref).location,
                            );
                        }
                    }
                }
            }
        }
        _ => {
            crerr = CrErr::TooMany; /* too many dotted names */
        }
    }

    /*
     * Now give the PostParseColumnRefHook, if any, a chance.  We pass the
     * translation-so-far so that it can throw an error if it wishes in the
     * case that it has a conflicting interpretation of the ColumnRef. (If it
     * just translates anyway, we'll throw an error, because we can't undo
     * whatever effects the preceding steps may have had on the pstate.) If it
     * returns NULL, use the standard translation, or throw a suitable error
     * if there is none.
     */
    if let Some(hook) = (*pstate).p_post_columnref_hook {
        let hookresult = hook(pstate, cref as *mut c_void, node);
        if node.is_null() {
            node = hookresult;
        } else if !hookresult.is_null() {
            ereport!(
                ERROR,
                errmsg!("column reference \"{}\" is ambiguous",
                         cstr_to_str(NameListToString((*cref).fields)))
                /* C also: errcode, errdetail, errhint, parser_errposition */
            );
        }
    }

    /*
     * Throw error if no translation found.
     */
    if node.is_null() {
        match crerr {
            CrErr::NoColumn => {
                errorMissingColumn(pstate, relname, colname, (*cref).location);
            }
            CrErr::NoRte => {
                errorMissingRTE(
                    pstate,
                    makeRangeVar(nspname as *mut c_char, relname as *mut c_char, (*cref).location),
                );
            }
            CrErr::WrongDb => {
                ereport!(
                    ERROR,
                    errmsg!("cross-database references are not implemented: {}",
                             cstr_to_str(NameListToString((*cref).fields)))
                    /* C also: errcode, errdetail, errhint, parser_errposition */
                );
            }
            CrErr::TooMany => {
                ereport!(
                    ERROR,
                    errmsg!("improper qualified name (too many dotted names): {}",
                             cstr_to_str(NameListToString((*cref).fields)))
                    /* C also: errcode, errdetail, errhint, parser_errposition */
                );
            }
        }
    }

    node
}

unsafe fn transformParamRef(
    pstate: *mut ParseState,
    pref: *mut ParamRef,
) -> *mut Node {
    let result: *mut Node;

    /*
     * The core parser knows nothing about Params.  If a hook is supplied,
     * call it.  If not, or if the hook returns NULL, throw a generic error.
     */
    if let Some(hook) = (*pstate).p_paramref_hook {
        result = hook(pstate, pref as *mut c_void);
    } else {
        result = std::ptr::null_mut();
    }

    if result.is_null() {
        ereport!(
            ERROR,
            errmsg!("there is no parameter ${}", (*pref).number)
            /* C also: errcode, errdetail, errhint, parser_errposition */
        );
    }

    result
}

/* Test whether an a_expr is a plain NULL constant or not */
unsafe fn exprIsNullConstant(arg: *mut Node) -> bool {
    if !arg.is_null() && IsA!(arg, T_A_Const) {
        let con = arg as *mut A_Const;
        if (*con).isnull {
            return true;
        }
    }
    false
}

unsafe fn transformAExprOp(pstate: *mut ParseState, a: *mut A_Expr) -> *mut Node {
    let mut lexpr = (*a).lexpr;
    let mut rexpr = (*a).rexpr;
    let result: *mut Node;

    /*
     * Special-case "foo = NULL" and "NULL = foo" for compatibility with
     * standards-broken products (like Microsoft's).  Turn these into IS NULL
     * exprs. (If either side is a CaseTestExpr, then the expression was
     * generated internally from a CASE-WHEN expression, and
     * transform_null_equals does not apply.)
     */
    if Transform_null_equals
        && list_length((*a).name) == 1
        && strcmp(strVal!(linitial((*a).name) as *mut Node), cstr!("=")) == 0
        && (exprIsNullConstant(lexpr) || exprIsNullConstant(rexpr))
        && (!IsA!(lexpr, T_CaseTestExpr) && !IsA!(rexpr, T_CaseTestExpr))
    {
        let n = makeNode!(NullTest, T_NullTest) as *mut NullTest;

        (*n).nulltesttype = NullTestType::IS_NULL;
        (*n).location = (*a).location;

        if exprIsNullConstant(lexpr) {
            (*n).arg = rexpr as *mut Expr;
        } else {
            (*n).arg = lexpr as *mut Expr;
        }

        result = transformExprRecurse(pstate, n as *mut Node);
    } else if !lexpr.is_null()
        && IsA!(lexpr, T_RowExpr)
        && !rexpr.is_null()
        && IsA!(rexpr, T_SubLink)
        && (*(rexpr as *mut SubLink)).subLinkType == SubLinkType::EXPR_SUBLINK
    {
        /*
         * Convert "row op subselect" into a ROWCOMPARE sublink. Formerly the
         * grammar did this, but now that a row construct is allowed anywhere
         * in expressions, it's easier to do it here.
         */
        let s = rexpr as *mut SubLink;

        (*s).subLinkType = SubLinkType::ROWCOMPARE_SUBLINK;
        (*s).testexpr = lexpr;
        (*s).operName = (*a).name;
        (*s).location = (*a).location;
        result = transformExprRecurse(pstate, s as *mut Node);
    } else if !lexpr.is_null()
        && IsA!(lexpr, T_RowExpr)
        && !rexpr.is_null()
        && IsA!(rexpr, T_RowExpr)
    {
        /* ROW() op ROW() is handled specially */
        lexpr = transformExprRecurse(pstate, lexpr);
        rexpr = transformExprRecurse(pstate, rexpr);

        result = make_row_comparison_op(
            pstate,
            (*a).name,
            (*(castNode!(RowExpr, T_RowExpr, lexpr) as *mut RowExpr)).args,
            (*(castNode!(RowExpr, T_RowExpr, rexpr) as *mut RowExpr)).args,
            (*a).location,
        );
    } else {
        /* Ordinary scalar operator */
        let last_srf = (*pstate).p_last_srf;

        lexpr = transformExprRecurse(pstate, lexpr);
        rexpr = transformExprRecurse(pstate, rexpr);

        result = make_op(
            pstate,
            (*a).name,
            lexpr,
            rexpr,
            last_srf,
            (*a).location,
        ) as *mut Node;
    }

    result
}

unsafe fn transformAExprOpAny(pstate: *mut ParseState, a: *mut A_Expr) -> *mut Node {
    let lexpr = transformExprRecurse(pstate, (*a).lexpr);
    let rexpr = transformExprRecurse(pstate, (*a).rexpr);

    make_scalar_array_op(pstate, (*a).name, true, lexpr, rexpr, (*a).location) as *mut Node
}

unsafe fn transformAExprOpAll(pstate: *mut ParseState, a: *mut A_Expr) -> *mut Node {
    let lexpr = transformExprRecurse(pstate, (*a).lexpr);
    let rexpr = transformExprRecurse(pstate, (*a).rexpr);

    make_scalar_array_op(pstate, (*a).name, false, lexpr, rexpr, (*a).location) as *mut Node
}

unsafe fn transformAExprDistinct(pstate: *mut ParseState, a: *mut A_Expr) -> *mut Node {
    let mut lexpr = (*a).lexpr;
    let mut rexpr = (*a).rexpr;
    let mut result: *mut Node;

    /*
     * If either input is an undecorated NULL literal, transform to a NullTest
     * on the other input. That's simpler to process than a full DistinctExpr,
     * and it avoids needing to require that the datatype have an = operator.
     */
    if exprIsNullConstant(rexpr) {
        return make_nulltest_from_distinct(pstate, a, lexpr);
    }
    if exprIsNullConstant(lexpr) {
        return make_nulltest_from_distinct(pstate, a, rexpr);
    }

    lexpr = transformExprRecurse(pstate, lexpr);
    rexpr = transformExprRecurse(pstate, rexpr);

    if !lexpr.is_null()
        && IsA!(lexpr, T_RowExpr)
        && !rexpr.is_null()
        && IsA!(rexpr, T_RowExpr)
    {
        /* ROW() op ROW() is handled specially */
        result = make_row_distinct_op(
            pstate,
            (*a).name,
            lexpr as *mut RowExpr,
            rexpr as *mut RowExpr,
            (*a).location,
        );
    } else {
        /* Ordinary scalar operator */
        result = make_distinct_op(pstate, (*a).name, lexpr, rexpr, (*a).location)
            as *mut Node;
    }

    /*
     * If it's NOT DISTINCT, we first build a DistinctExpr and then stick a
     * NOT on top.
     */
    if (*a).kind == A_ExprKind::AEXPR_NOT_DISTINCT {
        result = makeBoolExpr(BoolExprType::NOT_EXPR, list_make1!(result), (*a).location)
            as *mut Node;
    }

    result
}

unsafe fn transformAExprNullIf(pstate: *mut ParseState, a: *mut A_Expr) -> *mut Node {
    let lexpr = transformExprRecurse(pstate, (*a).lexpr);
    let rexpr = transformExprRecurse(pstate, (*a).rexpr);
    let result: *mut OpExpr;

    result = make_op(
        pstate,
        (*a).name,
        lexpr,
        rexpr,
        (*pstate).p_last_srf,
        (*a).location,
    ) as *mut OpExpr;

    /*
     * The comparison operator itself should yield boolean ...
     */
    if (*result).opresulttype != BOOLOID {
        ereport!(
            ERROR,
            errmsg!("set-returning functions are not allowed in this context")
            /* C also: errcode, parser_errposition */
        );
    }
    if (*result).opretset {
        ereport!(
            ERROR,
            errmsg!("set-returning functions are not allowed in this context")
            /* C also: errcode, parser_errposition */
        );
    }

    /*
     * ... but the NullIfExpr will yield the first operand's type.
     */
    (*result).opresulttype = exprType(linitial((*result).args) as *const Node);

    /*
     * We rely on NullIfExpr and OpExpr being the same struct
     */
    NodeSetTag!(result, T_NullIfExpr);

    result as *mut Node
}

unsafe fn transformAExprIn(pstate: *mut ParseState, a: *mut A_Expr) -> *mut Node {
    let mut result: *mut Node = std::ptr::null_mut();
    let lexpr: *mut Node;
    let mut rexprs: *mut List;
    let mut rvars: *mut List;
    let mut rnonvars: *mut List;
    let use_or: bool;
    let mut has_rvars = false;

    /*
     * If the operator is <>, combine with AND not OR.
     */
    if strcmp(strVal!(linitial((*a).name) as *mut Node), cstr!("<>")) == 0 {
        use_or = false;
    } else {
        use_or = true;
    }

    /*
     * We try to generate a ScalarArrayOpExpr from IN/NOT IN, but this is only
     * possible if there is a suitable array type available.  If not, we fall
     * back to a boolean condition tree with multiple copies of the lefthand
     * expression.  Also, any IN-list items that contain Vars are handled as
     * separate boolean conditions, because that gives the planner more scope
     * for optimization on such clauses.
     *
     * First step: transform all the inputs, and detect whether any contain
     * Vars.
     */
    lexpr = transformExprRecurse(pstate, (*a).lexpr);
    rexprs = NIL;
    rvars = NIL;
    rnonvars = NIL;
    foreach!(l_cell, ((*a).rexpr as *mut List), {
        let rexpr = transformExprRecurse(pstate, lfirst(current_cell!(l_cell)) as *mut Node);

        rexprs = lappend(rexprs, rexpr as *mut c_void);
        if contain_vars_of_level(rexpr, 0) {
            rvars = lappend(rvars, rexpr as *mut c_void);
            has_rvars = true;
        } else {
            rnonvars = lappend(rnonvars, rexpr as *mut c_void);
        }
    });

    /*
     * ScalarArrayOpExpr is only going to be useful if there's more than one
     * non-Var righthand item.
     */
    if list_length(rnonvars) > 1 {
        let allexprs: *mut List;
        let scalar_type: Oid;
        let array_type: Oid;

        /*
         * Try to select a common type for the array elements.  Note that
         * since the LHS' type is first in the list, it will be preferred when
         * there is doubt (eg, when all the RHS items are unknown literals).
         *
         * Note: use list_concat here not lcons, to avoid damaging rnonvars.
         */
        allexprs = list_concat(list_make1!(lexpr), rnonvars);
        scalar_type = select_common_type(pstate, allexprs, std::ptr::null(), std::ptr::null_mut());

        /* We have to verify that the selected type actually works */
        let scalar_type = if OidIsValid(scalar_type) && !verify_common_type(scalar_type, allexprs) {
            InvalidOid!()
        } else {
            scalar_type
        };

        /*
         * Do we have an array type to use?  Aside from the case where there
         * isn't one, we don't risk using ScalarArrayOpExpr when the common
         * type is RECORD, because the RowExpr comparison logic below can cope
         * with some cases of non-identical row types.
         */
        if OidIsValid(scalar_type) && scalar_type != RECORDOID {
            array_type = get_array_type(scalar_type);
        } else {
            array_type = InvalidOid!();
        }

        if array_type != InvalidOid!() {
            /*
             * OK: coerce all the right-hand non-Var inputs to the common type
             * and build an ArrayExpr for them.
             */
            let mut aexprs: *mut List = NIL;
            let newa = makeNode!(ArrayExpr, T_ArrayExpr) as *mut ArrayExpr;

            foreach!(l_cell, rnonvars, {
                let mut rexpr = lfirst(current_cell!(l_cell)) as *mut Node;

                rexpr = coerce_to_common_type(pstate, rexpr, scalar_type, cstr!("IN"));
                aexprs = lappend(aexprs, rexpr as *mut c_void);
            });

            (*newa).array_typeid = array_type;
            /* array_collid will be set by parse_collate.c */
            (*newa).element_typeid = scalar_type;
            (*newa).elements = aexprs;
            (*newa).multidims = false;
            (*newa).location = -1;

            /*
             * If the IN expression contains Vars, disable query jumbling
             * squashing.  Vars cannot be safely jumbled.
             */
            (*newa).list_start = if has_rvars { -1 } else { (*a).rexpr_list_start };
            (*newa).list_end = if has_rvars { -1 } else { (*a).rexpr_list_end };

            result = make_scalar_array_op(
                pstate,
                (*a).name,
                use_or,
                lexpr,
                newa as *mut Node,
                (*a).location,
            ) as *mut Node;

            /* Consider only the Vars (if any) in the loop below */
            rexprs = rvars;
        }
    }

    /*
     * Must do it the hard way, ie, with a boolean expression tree.
     */
    foreach!(l_cell, rexprs, {
        let rexpr = lfirst(current_cell!(l_cell)) as *mut Node;
        let cmp: *mut Node;

        if IsA!(lexpr, T_RowExpr) && IsA!(rexpr, T_RowExpr) {
            /* ROW() op ROW() is handled specially */
            cmp = make_row_comparison_op(
                pstate,
                (*a).name,
                copyObject((*(lexpr as *mut RowExpr)).args),
                (*(rexpr as *mut RowExpr)).args,
                (*a).location,
            );
        } else {
            /* Ordinary scalar operator */
            cmp = make_op(
                pstate,
                (*a).name,
                copyObject(lexpr),
                rexpr,
                (*pstate).p_last_srf,
                (*a).location,
            ) as *mut Node;
        }

        let cmp = coerce_to_boolean(pstate, cmp, cstr!("IN"));
        if result.is_null() {
            result = cmp;
        } else {
            result = makeBoolExpr(
                if use_or { BoolExprType::OR_EXPR } else { BoolExprType::AND_EXPR },
                list_make2!(result, cmp),
                (*a).location,
            ) as *mut Node;
        }
    });

    result
}

unsafe fn transformAExprBetween(pstate: *mut ParseState, a: *mut A_Expr) -> *mut Node {
    let aexpr: *mut Node;
    let bexpr: *mut Node;
    let cexpr: *mut Node;
    let result: *mut Node;
    let sub1: *mut Node;
    let sub2: *mut Node;
    let mut args: *mut List;

    /* Deconstruct A_Expr into three subexprs */
    aexpr = (*a).lexpr;
    args = castNode!(List, T_List, (*a).rexpr) as *mut List;
    Assert!(list_length(args) == 2);
    bexpr = linitial(args) as *mut Node;
    cexpr = lsecond(args) as *mut Node;

    /*
     * Build the equivalent comparison expression.  Make copies of
     * multiply-referenced subexpressions for safety.  (XXX this is really
     * wrong since it results in multiple runtime evaluations of what may be
     * volatile expressions ...)
     *
     * Ideally we would not use hard-wired operators here but instead use
     * opclasses.  However, mixed data types and other issues make this
     * difficult:
     * http://archives.postgresql.org/pgsql-hackers/2008-08/msg01142.php
     */
    match (*a).kind {
        A_ExprKind::AEXPR_BETWEEN => {
            args = list_make2!(
                makeSimpleA_Expr(A_ExprKind::AEXPR_OP, cstr!(">=") as *mut c_char, aexpr, bexpr, (*a).location),
                makeSimpleA_Expr(A_ExprKind::AEXPR_OP, cstr!("<=") as *mut c_char, copyObject(aexpr), cexpr, (*a).location)
            );
            result = makeBoolExpr(BoolExprType::AND_EXPR, args, (*a).location) as *mut Node;
        }
        A_ExprKind::AEXPR_NOT_BETWEEN => {
            args = list_make2!(
                makeSimpleA_Expr(A_ExprKind::AEXPR_OP, cstr!("<") as *mut c_char, aexpr, bexpr, (*a).location),
                makeSimpleA_Expr(A_ExprKind::AEXPR_OP, cstr!(">") as *mut c_char, copyObject(aexpr), cexpr, (*a).location)
            );
            result = makeBoolExpr(BoolExprType::OR_EXPR, args, (*a).location) as *mut Node;
        }
        A_ExprKind::AEXPR_BETWEEN_SYM => {
            args = list_make2!(
                makeSimpleA_Expr(A_ExprKind::AEXPR_OP, cstr!(">=") as *mut c_char, aexpr, bexpr, (*a).location),
                makeSimpleA_Expr(A_ExprKind::AEXPR_OP, cstr!("<=") as *mut c_char, copyObject(aexpr), cexpr, (*a).location)
            );
            sub1 = makeBoolExpr(BoolExprType::AND_EXPR, args, (*a).location) as *mut Node;
            args = list_make2!(
                makeSimpleA_Expr(A_ExprKind::AEXPR_OP, cstr!(">=") as *mut c_char, copyObject(aexpr), copyObject(cexpr), (*a).location),
                makeSimpleA_Expr(A_ExprKind::AEXPR_OP, cstr!("<=") as *mut c_char, copyObject(aexpr), copyObject(bexpr), (*a).location)
            );
            sub2 = makeBoolExpr(BoolExprType::AND_EXPR, args, (*a).location) as *mut Node;
            args = list_make2!(sub1, sub2);
            result = makeBoolExpr(BoolExprType::OR_EXPR, args, (*a).location) as *mut Node;
        }
        A_ExprKind::AEXPR_NOT_BETWEEN_SYM => {
            args = list_make2!(
                makeSimpleA_Expr(A_ExprKind::AEXPR_OP, cstr!("<") as *mut c_char, aexpr, bexpr, (*a).location),
                makeSimpleA_Expr(A_ExprKind::AEXPR_OP, cstr!(">") as *mut c_char, copyObject(aexpr), cexpr, (*a).location)
            );
            sub1 = makeBoolExpr(BoolExprType::OR_EXPR, args, (*a).location) as *mut Node;
            args = list_make2!(
                makeSimpleA_Expr(A_ExprKind::AEXPR_OP, cstr!("<") as *mut c_char, copyObject(aexpr), copyObject(cexpr), (*a).location),
                makeSimpleA_Expr(A_ExprKind::AEXPR_OP, cstr!(">") as *mut c_char, copyObject(aexpr), copyObject(bexpr), (*a).location)
            );
            sub2 = makeBoolExpr(BoolExprType::OR_EXPR, args, (*a).location) as *mut Node;
            args = list_make2!(sub1, sub2);
            result = makeBoolExpr(BoolExprType::AND_EXPR, args, (*a).location) as *mut Node;
        }
        _ => {
            elog!(ERROR, "unrecognized A_Expr kind: {}", (*a).kind as c_int);
            result = std::ptr::null_mut(); /* keep compiler quiet */
        }
    }

    transformExprRecurse(pstate, result)
}

unsafe fn transformMergeSupportFunc(
    pstate: *mut ParseState,
    f: *mut MergeSupportFunc,
) -> *mut Node {
    /*
     * All we need to do is check that we're in the RETURNING list of a MERGE
     * command.  If so, we just return the node as-is.
     */
    if (*pstate).p_expr_kind != EXPR_KIND_MERGE_RETURNING {
        let mut parent_pstate = (*pstate).parentParseState;

        while !parent_pstate.is_null()
            && (*parent_pstate).p_expr_kind != EXPR_KIND_MERGE_RETURNING
        {
            parent_pstate = (*parent_pstate).parentParseState;
        }

        if parent_pstate.is_null() {
            ereport!(ERROR, errmsg!("MERGE_ACTION() can only be used in the RETURNING list of a MERGE command")) /* C also: errcode, parser_errposition */;
        }
    }

    f as *mut Node
}

unsafe fn transformBoolExpr(pstate: *mut ParseState, a: *mut BoolExpr) -> *mut Node {
    let mut args: *mut List = NIL;
    let opname: *const c_char;

    #[allow(unreachable_patterns)]
    match (*a).boolop {
        BoolExprType::AND_EXPR => {
            opname = cstr!("AND");
        }
        BoolExprType::OR_EXPR => {
            opname = cstr!("OR");
        }
        BoolExprType::NOT_EXPR => {
            opname = cstr!("NOT");
        }
        _ => {
            elog!(ERROR, "unrecognized boolop: {}", (*a).boolop as c_int);
            opname = std::ptr::null(); /* keep compiler quiet */
        }
    }

    foreach!(lc, (*a).args, {
        let mut arg = lfirst(current_cell!(lc)) as *mut Node;

        arg = transformExprRecurse(pstate, arg);
        arg = coerce_to_boolean(pstate, arg, opname);
        args = lappend(args, arg as *mut c_void);
    });

    makeBoolExpr((*a).boolop, args, (*a).location) as *mut Node
}

unsafe fn transformFuncCall(pstate: *mut ParseState, fn_: *mut FuncCall) -> *mut Node {
    let last_srf = (*pstate).p_last_srf;
    let mut targs: *mut List = NIL;

    /* Transform the list of arguments ... */
    foreach!(args_cell, (*fn_).args, {
        targs = lappend(
            targs,
            transformExprRecurse(pstate, lfirst(current_cell!(args_cell)) as *mut Node) as *mut c_void,
        );
    });

    /*
     * When WITHIN GROUP is used, we treat its ORDER BY expressions as
     * additional arguments to the function, for purposes of function lookup
     * and argument type coercion.  So, transform each such expression and add
     * them to the targs list.  We don't explicitly mark where each argument
     * came from, but ParseFuncOrColumn can tell what's what by reference to
     * list_length(fn->agg_order).
     */
    if (*fn_).agg_within_group {
        Assert!(!(*fn_).agg_order.is_null());
        foreach!(args_cell, (*fn_).agg_order, {
            let arg = lfirst(current_cell!(args_cell)) as *mut SortBy;

            targs = lappend(
                targs,
                transformExpr(pstate, (*arg).node, EXPR_KIND_ORDER_BY) as *mut c_void,
            );
        });
    }

    /* ... and hand off to ParseFuncOrColumn */
    ParseFuncOrColumn(
        pstate,
        (*fn_).funcname,
        targs,
        last_srf,
        fn_,
        false,
        (*fn_).location,
    )
}

unsafe fn transformMultiAssignRef(
    pstate: *mut ParseState,
    maref: *mut MultiAssignRef,
) -> *mut Node {
    let sublink: *mut SubLink;
    let rexpr: *mut RowExpr;
    let qtree: *mut Query;
    let tle: *mut TargetEntry;

    /* We should only see this in first-stage processing of UPDATE tlists */
    Assert!((*pstate).p_expr_kind == EXPR_KIND_UPDATE_SOURCE);

    /* We only need to transform the source if this is the first column */
    if (*maref).colno == 1 {
        /*
         * For now, we only allow EXPR SubLinks and RowExprs as the source of
         * an UPDATE multiassignment.  This is sufficient to cover interesting
         * cases; at worst, someone would have to write (SELECT * FROM expr)
         * to expand a composite-returning expression of another form.
         */
        if IsA!((*maref).source, T_SubLink)
            && (*((*maref).source as *mut SubLink)).subLinkType == SubLinkType::EXPR_SUBLINK
        {
            /* Relabel it as a MULTIEXPR_SUBLINK */
            let sl = (*maref).source as *mut SubLink;
            (*sl).subLinkType = SubLinkType::MULTIEXPR_SUBLINK;
            /* And transform it */
            let sl = transformExprRecurse(pstate, sl as *mut Node) as *mut SubLink;

            let qt = castNode!(Query, T_Query, (*sl).subselect) as *mut Query;

            /* Check subquery returns required number of columns */
            if count_nonjunk_tlist_entries((*qt).targetList) != (*maref).ncolumns {
                ereport!(
                    ERROR,
                    errmsg!("number of columns does not match number of values")
                    /* C also: errcode, errdetail, errhint, parser_errposition */
                );
            }

            /*
             * Build a resjunk tlist item containing the MULTIEXPR SubLink,
             * and add it to pstate->p_multiassign_exprs, whence it will later
             * get appended to the completed targetlist.  We needn't worry
             * about selecting a resno for it; transformUpdateStmt will do
             * that.
             */
            let tle_new = makeTargetEntry(sl as *mut Expr, 0, std::ptr::null_mut(), true);
            (*pstate).p_multiassign_exprs = lappend(
                (*pstate).p_multiassign_exprs,
                tle_new as *mut c_void,
            );

            /*
             * Assign a unique-within-this-targetlist ID to the MULTIEXPR
             * SubLink.  We can just use its position in the
             * p_multiassign_exprs list.
             */
            (*sl).subLinkId = list_length((*pstate).p_multiassign_exprs);

            // sublink/qtree used below -- reassign for the shared path
            let _ = sl; // consumed
            // fall through to shared emit code below
            let tle_ref =
                llast((*pstate).p_multiassign_exprs) as *mut TargetEntry;

            // Emit the appropriate output expression for the current column
            if IsA!((*tle_ref).expr, T_SubLink) {
                let param: *mut Param;
                let sl2 = (*tle_ref).expr as *mut SubLink;
                Assert!((*sl2).subLinkType == SubLinkType::MULTIEXPR_SUBLINK);
                let qt2 = castNode!(Query, T_Query, (*sl2).subselect) as *mut Query;

                /* Build a Param representing the current subquery output column */
                let tle2 = list_nth((*qt2).targetList, (*maref).colno - 1) as *mut TargetEntry;
                Assert!(!(*tle2).resjunk);

                param = makeNode!(Param, T_Param) as *mut Param;
                (*param).paramkind = ParamKind::PARAM_MULTIEXPR;
                (*param).paramid = ((*sl2).subLinkId << 16) | (*maref).colno;
                (*param).paramtype = exprType((*tle2).expr as *const Node);
                (*param).paramtypmod = exprTypmod((*tle2).expr as *const Node);
                (*param).paramcollid = exprCollation((*tle2).expr as *const Node);
                (*param).location = exprLocation((*tle2).expr as *const Node);

                return param as *mut Node;
            }

            if IsA!((*tle_ref).expr, T_RowExpr) {
                let re = (*tle_ref).expr as *mut RowExpr;
                let result = list_nth((*re).args, (*maref).colno - 1) as *mut Node;

                if (*maref).colno == (*maref).ncolumns {
                    (*pstate).p_multiassign_exprs =
                        list_delete_last((*pstate).p_multiassign_exprs);
                }

                return result;
            }

            elog!(ERROR, "unexpected expr type in multiassign list");
            return std::ptr::null_mut();
        } else if IsA!((*maref).source, T_RowExpr) {
            /* Transform the RowExpr, allowing SetToDefault items */
            let re = transformRowExpr(
                pstate,
                (*maref).source as *mut ParseRowExpr,
                true,
            ) as *mut RowExpr;

            /* Check it returns required number of columns */
            if list_length((*re).args) != (*maref).ncolumns {
                ereport!(
                    ERROR,
                    errmsg!("number of columns does not match number of values")
                    /* C also: errcode, errdetail, errhint, parser_errposition */
                );
            }

            /*
             * Temporarily append it to p_multiassign_exprs, so we can get it
             * back when we come back here for additional columns.
             */
            let tle_new = makeTargetEntry(re as *mut Expr, 0, std::ptr::null_mut(), true);
            (*pstate).p_multiassign_exprs = lappend(
                (*pstate).p_multiassign_exprs,
                tle_new as *mut c_void,
            );
        } else {
            ereport!(
                ERROR,
                errmsg!("source for a multiple-column UPDATE item must be a sub-SELECT or ROW() expression")
                /* C also: errcode, errdetail, errhint, parser_errposition */
            );
        }
    }

    /*
     * Second or later column in a multiassignment.  Re-fetch the
     * transformed SubLink or RowExpr, which we assume is still the last
     * entry in p_multiassign_exprs.
     */
    Assert!(!(*pstate).p_multiassign_exprs.is_null());
    let tle_last = llast((*pstate).p_multiassign_exprs) as *mut TargetEntry;

    /*
     * Emit the appropriate output expression for the current column
     */
    if IsA!((*tle_last).expr, T_SubLink) {
        let param: *mut Param;
        let sl = (*tle_last).expr as *mut SubLink;
        Assert!((*sl).subLinkType == SubLinkType::MULTIEXPR_SUBLINK);
        let qt = castNode!(Query, T_Query, (*sl).subselect) as *mut Query;

        /* Build a Param representing the current subquery output column */
        let tle2 = list_nth((*qt).targetList, (*maref).colno - 1) as *mut TargetEntry;
        Assert!(!(*tle2).resjunk);

        param = makeNode!(Param, T_Param) as *mut Param;
        (*param).paramkind = ParamKind::PARAM_MULTIEXPR;
        (*param).paramid = ((*sl).subLinkId << 16) | (*maref).colno;
        (*param).paramtype = exprType((*tle2).expr as *const Node);
        (*param).paramtypmod = exprTypmod((*tle2).expr as *const Node);
        (*param).paramcollid = exprCollation((*tle2).expr as *const Node);
        (*param).location = exprLocation((*tle2).expr as *const Node);

        return param as *mut Node;
    }

    if IsA!((*tle_last).expr, T_RowExpr) {
        let re = (*tle_last).expr as *mut RowExpr;

        let result = list_nth((*re).args, (*maref).colno - 1) as *mut Node;

        /*
         * If we're at the last column, delete the RowExpr from
         * p_multiassign_exprs; we don't need it anymore, and don't want it in
         * the finished UPDATE tlist.  We assume this is still the last entry
         * in p_multiassign_exprs.
         */
        if (*maref).colno == (*maref).ncolumns {
            (*pstate).p_multiassign_exprs =
                list_delete_last((*pstate).p_multiassign_exprs);
        }

        return result;
    }

    elog!(ERROR, "unexpected expr type in multiassign list");
    std::ptr::null_mut() /* keep compiler quiet */
}

unsafe fn transformCaseExpr(pstate: *mut ParseState, c: *mut CaseExpr) -> *mut Node {
    let newc = makeNode!(CaseExpr, T_CaseExpr) as *mut CaseExpr;
    let last_srf = (*pstate).p_last_srf;
    let mut arg: *mut Node;
    let placeholder: *mut CaseTestExpr;
    let mut newargs: *mut List = NIL;
    let mut resultexprs: *mut List = NIL;
    let defresult: *mut Node;
    let ptype: Oid;

    /* transform the test expression, if any */
    arg = transformExprRecurse(pstate, (*c).arg as *mut Node);

    /* generate placeholder for test expression */
    if !arg.is_null() {
        /*
         * If test expression is an untyped literal, force it to text. We have
         * to do something now because we won't be able to do this coercion on
         * the placeholder.  This is not as flexible as what was done in 7.4
         * and before, but it's good enough to handle the sort of silly coding
         * commonly seen.
         */
        if exprType(arg) == UNKNOWNOID {
            arg = coerce_to_common_type(pstate, arg, TEXTOID, cstr!("CASE"));
        }

        /*
         * Run collation assignment on the test expression so that we know
         * what collation to mark the placeholder with.  In principle we could
         * leave it to parse_collate.c to do that later, but propagating the
         * result to the CaseTestExpr would be unnecessarily complicated.
         */
        assign_expr_collations(pstate, arg);

        let ph = makeNode!(CaseTestExpr, T_CaseTestExpr) as *mut CaseTestExpr;
        (*ph).typeId = exprType(arg);
        (*ph).typeMod = exprTypmod(arg);
        (*ph).collation = exprCollation(arg);
        placeholder = ph;
    } else {
        placeholder = std::ptr::null_mut();
    }

    (*newc).arg = arg as *mut Expr;

    /* transform the list of arguments */
    foreach!(l_cell, (*c).args, {
        let w = lfirst_node!(CaseWhen, T_CaseWhen, current_cell!(l_cell)) as *mut CaseWhen;
        let neww = makeNode!(CaseWhen, T_CaseWhen) as *mut CaseWhen;
        let mut warg: *mut Node;

        warg = (*w).expr as *mut Node;
        if !placeholder.is_null() {
            /* shorthand form was specified, so expand... */
            warg = makeSimpleA_Expr(
                A_ExprKind::AEXPR_OP,
                cstr!("=") as *mut c_char,
                placeholder as *mut Node,
                warg,
                (*w).location,
            ) as *mut Node;
        }
        (*neww).expr = transformExprRecurse(pstate, warg) as *mut Expr;

        (*neww).expr = coerce_to_boolean(
            pstate,
            (*neww).expr as *mut Node,
            cstr!("CASE/WHEN"),
        ) as *mut Expr;

        warg = (*w).result as *mut Node;
        (*neww).result = transformExprRecurse(pstate, warg) as *mut Expr;
        (*neww).location = (*w).location;

        newargs = lappend(newargs, neww as *mut c_void);
        resultexprs = lappend(resultexprs, (*neww).result as *mut c_void);
    });

    (*newc).args = newargs;

    /* transform the default clause */
    let defresult_raw = (*c).defresult;
    let defresult: *mut Node = if defresult_raw.is_null() {
        let n = makeNode!(A_Const, T_A_Const) as *mut A_Const;
        (*n).isnull = true;
        (*n).location = -1;
        n as *mut Node
    } else {
        defresult_raw as *mut Node
    };
    (*newc).defresult = transformExprRecurse(pstate, defresult) as *mut Expr;

    /*
     * Note: default result is considered the most significant type in
     * determining preferred type. This is how the code worked before, but it
     * seems a little bogus to me --- tgl
     */
    resultexprs = lcons((*newc).defresult as *mut c_void, resultexprs);

    ptype = select_common_type(pstate, resultexprs, cstr!("CASE"), std::ptr::null_mut());
    Assert!(OidIsValid(ptype));
    (*newc).casetype = ptype;
    /* casecollid will be set by parse_collate.c */

    /* Convert default result clause, if necessary */
    (*newc).defresult = coerce_to_common_type(
        pstate,
        (*newc).defresult as *mut Node,
        ptype,
        cstr!("CASE/ELSE"),
    ) as *mut Expr;

    /* Convert when-clause results, if necessary */
    foreach!(l_cell, (*newc).args, {
        let w = lfirst(current_cell!(l_cell)) as *mut CaseWhen;

        (*w).result = coerce_to_common_type(
            pstate,
            (*w).result as *mut Node,
            ptype,
            cstr!("CASE/WHEN"),
        ) as *mut Expr;
    });

    /* if any subexpression contained a SRF, complain */
    if (*pstate).p_last_srf != last_srf {
        ereport!(
            ERROR,
            errmsg!("set-returning functions are not allowed in this context")
            /* C also: errcode, parser_errposition */
        );
    }

    (*newc).location = (*c).location;

    newc as *mut Node
}

unsafe fn transformSubLink(pstate: *mut ParseState, sublink: *mut SubLink) -> *mut Node {
    let result: *mut Node = sublink as *mut Node;
    let qtree: *mut Query;
    let mut err: *const c_char;

    /*
     * Check to see if the sublink is in an invalid place within the query. We
     * allow sublinks everywhere in SELECT/INSERT/UPDATE/DELETE/MERGE, but
     * generally not in utility statements.
     */
    err = std::ptr::null();
    match (*pstate).p_expr_kind {
        EXPR_KIND_NONE => {
            Assert!(false); /* can't happen */
        }
        EXPR_KIND_OTHER => {
            /* Accept sublink here; caller must throw error if wanted */
        }
        EXPR_KIND_JOIN_ON
        | EXPR_KIND_JOIN_USING
        | EXPR_KIND_FROM_SUBSELECT
        | EXPR_KIND_FROM_FUNCTION
        | EXPR_KIND_WHERE
        | EXPR_KIND_POLICY
        | EXPR_KIND_HAVING
        | EXPR_KIND_FILTER
        | EXPR_KIND_WINDOW_PARTITION
        | EXPR_KIND_WINDOW_ORDER
        | EXPR_KIND_WINDOW_FRAME_RANGE
        | EXPR_KIND_WINDOW_FRAME_ROWS
        | EXPR_KIND_WINDOW_FRAME_GROUPS
        | EXPR_KIND_SELECT_TARGET
        | EXPR_KIND_INSERT_TARGET
        | EXPR_KIND_UPDATE_SOURCE
        | EXPR_KIND_UPDATE_TARGET
        | EXPR_KIND_MERGE_WHEN
        | EXPR_KIND_GROUP_BY
        | EXPR_KIND_ORDER_BY
        | EXPR_KIND_DISTINCT_ON
        | EXPR_KIND_LIMIT
        | EXPR_KIND_OFFSET
        | EXPR_KIND_RETURNING
        | EXPR_KIND_MERGE_RETURNING
        | EXPR_KIND_VALUES
        | EXPR_KIND_VALUES_SINGLE
        | EXPR_KIND_CYCLE_MARK => {
            /* okay */
        }
        EXPR_KIND_CHECK_CONSTRAINT | EXPR_KIND_DOMAIN_CHECK => {
            err = cstr!("cannot use subquery in check constraint");
        }
        EXPR_KIND_COLUMN_DEFAULT | EXPR_KIND_FUNCTION_DEFAULT => {
            err = cstr!("cannot use subquery in DEFAULT expression");
        }
        EXPR_KIND_INDEX_EXPRESSION => {
            err = cstr!("cannot use subquery in index expression");
        }
        EXPR_KIND_INDEX_PREDICATE => {
            err = cstr!("cannot use subquery in index predicate");
        }
        EXPR_KIND_STATS_EXPRESSION => {
            err = cstr!("cannot use subquery in statistics expression");
        }
        EXPR_KIND_ALTER_COL_TRANSFORM => {
            err = cstr!("cannot use subquery in transform expression");
        }
        EXPR_KIND_EXECUTE_PARAMETER => {
            err = cstr!("cannot use subquery in EXECUTE parameter");
        }
        EXPR_KIND_TRIGGER_WHEN => {
            err = cstr!("cannot use subquery in trigger WHEN condition");
        }
        EXPR_KIND_PARTITION_BOUND => {
            err = cstr!("cannot use subquery in partition bound");
        }
        EXPR_KIND_PARTITION_EXPRESSION => {
            err = cstr!("cannot use subquery in partition key expression");
        }
        EXPR_KIND_CALL_ARGUMENT => {
            err = cstr!("cannot use subquery in CALL argument");
        }
        EXPR_KIND_COPY_WHERE => {
            err = cstr!("cannot use subquery in COPY FROM WHERE condition");
        }
        EXPR_KIND_GENERATED_COLUMN => {
            err = cstr!("cannot use subquery in column generation expression");
        }
        /*
         * There is intentionally no default: case here, so that the
         * compiler will warn if we add a new ParseExprKind without
         * extending this switch.  If we do see an unrecognized value at
         * runtime, the behavior will be the same as for EXPR_KIND_OTHER,
         * which is sane anyway.
         */
        #[allow(unreachable_patterns)]
        _ => {}
    }
    if !err.is_null() {
        ereport!(
            ERROR,
            errmsg_internal!("{}", std::ffi::CStr::from_ptr(err).to_string_lossy())
            /* C also: errcode, errdetail, errhint, parser_errposition */
        );
    }

    (*pstate).p_hasSubLinks = true;

    /*
     * OK, let's transform the sub-SELECT.
     */
    qtree = parse_sub_analyze(
        (*sublink).subselect,
        pstate,
        std::ptr::null_mut(),
        false,
        true,
    );

    /*
     * Check that we got a SELECT.  Anything else should be impossible given
     * restrictions of the grammar, but check anyway.
     */
    if !IsA!(qtree, T_Query) || (*qtree).commandType != crate::nodes::nodes::CmdType::CMD_SELECT
    {
        elog!(ERROR, "unexpected non-SELECT command in SubLink");
    }

    (*sublink).subselect = qtree as *mut Node;

    if (*sublink).subLinkType == SubLinkType::EXISTS_SUBLINK {
        /*
         * EXISTS needs no test expression or combining operator. These fields
         * should be null already, but make sure.
         */
        (*sublink).testexpr = std::ptr::null_mut();
        (*sublink).operName = NIL;
    } else if (*sublink).subLinkType == SubLinkType::EXPR_SUBLINK
        || (*sublink).subLinkType == SubLinkType::ARRAY_SUBLINK
    {
        /*
         * Make sure the subselect delivers a single column (ignoring resjunk
         * targets).
         */
        if count_nonjunk_tlist_entries((*qtree).targetList) != 1 {
            ereport!(
                ERROR,
                errmsg!("subquery must return only one column")
                /* C also: errcode, errdetail, errhint, parser_errposition */
            );
        }

        /*
         * EXPR and ARRAY need no test expression or combining operator. These
         * fields should be null already, but make sure.
         */
        (*sublink).testexpr = std::ptr::null_mut();
        (*sublink).operName = NIL;
    } else if (*sublink).subLinkType == SubLinkType::MULTIEXPR_SUBLINK {
        /* Same as EXPR case, except no restriction on number of columns */
        (*sublink).testexpr = std::ptr::null_mut();
        (*sublink).operName = NIL;
    } else {
        /* ALL, ANY, or ROWCOMPARE: generate row-comparing expression */
        let lefthand: *mut Node;
        let left_list: *mut List;
        let mut right_list: *mut List = NIL;

        /*
         * If the source was "x IN (select)", convert to "x = ANY (select)".
         */
        if (*sublink).operName.is_null() {
            (*sublink).operName = list_make1!(makeString(cstr!("=") as *mut c_char));
        }

        /*
         * Transform lefthand expression, and convert to a list
         */
        lefthand = transformExprRecurse(pstate, (*sublink).testexpr);
        if !lefthand.is_null() && IsA!(lefthand, T_RowExpr) {
            left_list = (*(lefthand as *mut RowExpr)).args;
        } else {
            left_list = list_make1!(lefthand);
        }

        /*
         * Build a list of PARAM_SUBLINK nodes representing the output columns
         * of the subquery.
         */
        foreach!(l_cell, (*qtree).targetList, {
            let tent = lfirst(current_cell!(l_cell)) as *mut TargetEntry;

            if (*tent).resjunk {
                // continue
            } else {
                let param = makeNode!(Param, T_Param) as *mut Param;
                (*param).paramkind = ParamKind::PARAM_SUBLINK;
                (*param).paramid = (*tent).resno as i32;
                (*param).paramtype = exprType((*tent).expr as *const Node);
                (*param).paramtypmod = exprTypmod((*tent).expr as *const Node);
                (*param).paramcollid = exprCollation((*tent).expr as *const Node);
                (*param).location = -1;

                right_list = lappend(right_list, param as *mut c_void);
            }
        });

        /*
         * We could rely on make_row_comparison_op to complain if the list
         * lengths differ, but we prefer to generate a more specific error
         * message.
         */
        if list_length(left_list) < list_length(right_list) {
            ereport!(
                ERROR,
                errmsg!("subquery has too many columns")
                /* C also: errcode, errdetail, errhint, parser_errposition */
            );
        }
        if list_length(left_list) > list_length(right_list) {
            ereport!(
                ERROR,
                errmsg!("subquery has too few columns")
                /* C also: errcode, errdetail, errhint, parser_errposition */
            );
        }

        /*
         * Identify the combining operator(s) and generate a suitable
         * row-comparison expression.
         */
        (*sublink).testexpr = make_row_comparison_op(
            pstate,
            (*sublink).operName,
            left_list,
            right_list,
            (*sublink).location,
        );
    }

    result
}

/*
 * transformArrayExpr
 *
 * If the caller specifies the target type, the resulting array will
 * be of exactly that type.  Otherwise we try to infer a common type
 * for the elements using select_common_type().
 */
unsafe fn transformArrayExpr(
    pstate: *mut ParseState,
    a: *mut A_ArrayExpr,
    mut array_type: Oid,
    mut element_type: Oid,
    typmod: int32,
) -> *mut Node {
    let newa = makeNode!(ArrayExpr, T_ArrayExpr) as *mut ArrayExpr;
    let mut newelems: *mut List = NIL;
    let mut newcoercedelems: *mut List = NIL;
    let coerce_type: Oid;
    let coerce_hard: bool;

    /*
     * Transform the element expressions
     *
     * Assume that the array is one-dimensional unless we find an array-type
     * element expression.
     */
    (*newa).multidims = false;
    foreach!(element_cell, (*a).elements, {
        let e = lfirst(current_cell!(element_cell)) as *mut Node;
        let newe: *mut Node;

        /*
         * If an element is itself an A_ArrayExpr, recurse directly so that we
         * can pass down any target type we were given.
         */
        if IsA!(e, T_A_ArrayExpr) {
            let ne = transformArrayExpr(
                pstate,
                e as *mut A_ArrayExpr,
                array_type,
                element_type,
                typmod,
            );
            /* we certainly have an array here */
            Assert!(array_type == InvalidOid!() || array_type == exprType(ne));
            (*newa).multidims = true;
            newelems = lappend(newelems, ne as *mut c_void);
        } else {
            let ne = transformExprRecurse(pstate, e);

            /*
             * Check for sub-array expressions, if we haven't already found
             * one.  Note we don't accept domain-over-array as a sub-array,
             * nor int2vector nor oidvector; those have constraints that don't
             * map well to being treated as a sub-array.
             */
            if !(*newa).multidims {
                let newetype = exprType(ne);

                if newetype != INT2VECTOROID
                    && newetype != OIDVECTOROID
                    && type_is_array(newetype)
                {
                    (*newa).multidims = true;
                }
            }
            newelems = lappend(newelems, ne as *mut c_void);
        }
    });

    /*
     * Select a target type for the elements.
     *
     * If we haven't been given a target array type, we must try to deduce a
     * common type based on the types of the individual elements present.
     */
    if OidIsValid(array_type) {
        /* Caller must ensure array_type matches element_type */
        Assert!(OidIsValid(element_type));
        coerce_type = if (*newa).multidims { array_type } else { element_type };
        coerce_hard = true;
    } else {
        /* Can't handle an empty array without a target type */
        if newelems.is_null() {
            ereport!(
                ERROR,
                errmsg!("cannot determine type of empty array")
                /* C also: errcode, errdetail, errhint, parser_errposition */
            );
        }

        /* Select a common type for the elements */
        let ct = select_common_type(pstate, newelems, cstr!("ARRAY"), std::ptr::null_mut());

        if (*newa).multidims {
            array_type = ct;
            element_type = get_element_type(array_type);
            if !OidIsValid(element_type) {
                ereport!(
                    ERROR,
                    errmsg!("could not find element type for data type {}",
                             cstr_to_str(format_type_be(array_type)))
                    /* C also: errcode, errdetail, errhint, parser_errposition */
                );
            }
        } else {
            element_type = ct;
            array_type = get_array_type(element_type);
            if !OidIsValid(array_type) {
                ereport!(
                    ERROR,
                    errmsg!("could not find array type for data type {}",
                             cstr_to_str(format_type_be(element_type)))
                    /* C also: errcode, errdetail, errhint, parser_errposition */
                );
            }
        }
        coerce_type = ct;
        coerce_hard = false;
    }

    /*
     * Coerce elements to target type
     *
     * If the array has been explicitly cast, then the elements are in turn
     * explicitly coerced.
     *
     * If the array's type was merely derived from the common type of its
     * elements, then the elements are implicitly coerced to the common type.
     * This is consistent with other uses of select_common_type().
     */
    foreach!(element_cell, newelems, {
        let e = lfirst(current_cell!(element_cell)) as *mut Node;
        let newe: *mut Node;

        if coerce_hard {
            let ne = coerce_to_target_type(
                pstate, e, exprType(e), coerce_type, typmod,
                COERCION_EXPLICIT, COERCE_EXPLICIT_CAST, -1,
            );
            if ne.is_null() {
                ereport!(
                    ERROR,
                    errmsg!("cannot cast type {} to {}",
                             cstr_to_str(format_type_be(exprType(e))),
                             cstr_to_str(format_type_be(coerce_type)))
                    /* C also: errcode, errdetail, errhint, parser_errposition */
                );
            }
            newcoercedelems = lappend(newcoercedelems, ne as *mut c_void);
        } else {
            let ne = coerce_to_common_type(pstate, e, coerce_type, cstr!("ARRAY"));
            newcoercedelems = lappend(newcoercedelems, ne as *mut c_void);
        }
    });

    (*newa).array_typeid = array_type;
    /* array_collid will be set by parse_collate.c */
    (*newa).element_typeid = element_type;
    (*newa).elements = newcoercedelems;
    (*newa).list_start = (*a).list_start;
    (*newa).list_end = (*a).list_end;
    (*newa).location = (*a).location;

    newa as *mut Node
}

unsafe fn transformRowExpr(
    pstate: *mut ParseState,
    r: *mut ParseRowExpr,
    allow_default: bool,
) -> *mut Node {
    let newr = makeNode!(RowExpr, T_RowExpr) as *mut RowExpr;
    let mut fnum: c_int;

    /* Transform the field expressions */
    (*newr).args = transformExpressionList(
        pstate,
        (*r).args,
        (*pstate).p_expr_kind,
        allow_default,
    );

    /* Disallow more columns than will fit in a tuple */
    if list_length((*newr).args) > MaxTupleAttributeNumber {
        ereport!(
            ERROR,
            errmsg!("ROW expressions can have at most {} entries", MaxTupleAttributeNumber)
            /* C also: errcode, errdetail, errhint, parser_errposition */
        );
    }

    /* Barring later casting, we consider the type RECORD */
    (*newr).row_typeid = RECORDOID;
    (*newr).row_format = CoercionForm::COERCE_IMPLICIT_CAST;

    /* ROW() has anonymous columns, so invent some field names */
    (*newr).colnames = NIL;
    fnum = 1;
    while fnum <= list_length((*newr).args) {
        let s = format!("f{}\0", fnum);
        let sptr = pstrdup(s.as_ptr() as *const c_char);
        (*newr).colnames = lappend((*newr).colnames, makeString(sptr) as *mut c_void);
        fnum += 1;
    }

    (*newr).location = (*r).location;

    newr as *mut Node
}

unsafe fn transformCoalesceExpr(
    pstate: *mut ParseState,
    c: *mut ParseCoalesceExpr,
) -> *mut Node {
    let newc = makeNode!(CoalesceExpr, T_CoalesceExpr) as *mut CoalesceExpr;
    let last_srf = (*pstate).p_last_srf;
    let mut newargs: *mut List = NIL;
    let mut newcoercedargs: *mut List = NIL;

    foreach!(args_cell, (*c).args, {
        let e = lfirst(current_cell!(args_cell)) as *mut Node;
        let newe = transformExprRecurse(pstate, e);
        newargs = lappend(newargs, newe as *mut c_void);
    });

    (*newc).coalescetype =
        select_common_type(pstate, newargs, cstr!("COALESCE"), std::ptr::null_mut());
    /* coalescecollid will be set by parse_collate.c */

    /* Convert arguments if necessary */
    foreach!(args_cell, newargs, {
        let e = lfirst(current_cell!(args_cell)) as *mut Node;
        let newe = coerce_to_common_type(pstate, e, (*newc).coalescetype, cstr!("COALESCE"));
        newcoercedargs = lappend(newcoercedargs, newe as *mut c_void);
    });

    /* if any subexpression contained a SRF, complain */
    if (*pstate).p_last_srf != last_srf {
        ereport!(
            ERROR,
            errmsg!("set-returning functions are not allowed in this context")
            /* C also: errcode, parser_errposition */
        );
    }

    (*newc).args = newcoercedargs;
    (*newc).location = (*c).location;
    newc as *mut Node
}

unsafe fn transformMinMaxExpr(
    pstate: *mut ParseState,
    m: *mut ParseMinMaxExpr,
) -> *mut Node {
    let newm = makeNode!(MinMaxExpr, T_MinMaxExpr) as *mut MinMaxExpr;
    let mut newargs: *mut List = NIL;
    let mut newcoercedargs: *mut List = NIL;
    let funcname: *const c_char = if (*m).op == IS_GREATEST {
        cstr!("GREATEST")
    } else {
        cstr!("LEAST")
    };

    (*newm).op = (*m).op;
    foreach!(args_cell, (*m).args, {
        let e = lfirst(current_cell!(args_cell)) as *mut Node;
        let newe = transformExprRecurse(pstate, e);
        newargs = lappend(newargs, newe as *mut c_void);
    });

    (*newm).minmaxtype =
        select_common_type(pstate, newargs, funcname, std::ptr::null_mut());
    /* minmaxcollid and inputcollid will be set by parse_collate.c */

    /* Convert arguments if necessary */
    foreach!(args_cell, newargs, {
        let e = lfirst(current_cell!(args_cell)) as *mut Node;
        let newe = coerce_to_common_type(pstate, e, (*newm).minmaxtype, funcname);
        newcoercedargs = lappend(newcoercedargs, newe as *mut c_void);
    });

    (*newm).args = newcoercedargs;
    (*newm).location = (*m).location;
    newm as *mut Node
}

unsafe fn transformSQLValueFunction(
    _pstate: *mut ParseState,
    svf: *mut SQLValueFunction,
) -> *mut Node {
    /*
     * All we need to do is insert the correct result type and (where needed)
     * validate the typmod, so we just modify the node in-place.
     */
    match (*svf).op {
        SQLValueFunctionOp::SVFOP_CURRENT_DATE => {
            (*svf).r#type = DATEOID;
        }
        SQLValueFunctionOp::SVFOP_CURRENT_TIME => {
            (*svf).r#type = TIMETZOID;
        }
        SQLValueFunctionOp::SVFOP_CURRENT_TIME_N => {
            (*svf).r#type = TIMETZOID;
            (*svf).typmod = anytime_typmod_check(true, (*svf).typmod);
        }
        SQLValueFunctionOp::SVFOP_CURRENT_TIMESTAMP => {
            (*svf).r#type = TIMESTAMPTZOID;
        }
        SQLValueFunctionOp::SVFOP_CURRENT_TIMESTAMP_N => {
            (*svf).r#type = TIMESTAMPTZOID;
            (*svf).typmod = anytimestamp_typmod_check(true, (*svf).typmod);
        }
        SQLValueFunctionOp::SVFOP_LOCALTIME => {
            (*svf).r#type = TIMEOID;
        }
        SQLValueFunctionOp::SVFOP_LOCALTIME_N => {
            (*svf).r#type = TIMEOID;
            (*svf).typmod = anytime_typmod_check(false, (*svf).typmod);
        }
        SQLValueFunctionOp::SVFOP_LOCALTIMESTAMP => {
            (*svf).r#type = TIMESTAMPOID;
        }
        SQLValueFunctionOp::SVFOP_LOCALTIMESTAMP_N => {
            (*svf).r#type = TIMESTAMPOID;
            (*svf).typmod = anytimestamp_typmod_check(false, (*svf).typmod);
        }
        SQLValueFunctionOp::SVFOP_CURRENT_ROLE
        | SQLValueFunctionOp::SVFOP_CURRENT_USER
        | SQLValueFunctionOp::SVFOP_USER
        | SQLValueFunctionOp::SVFOP_SESSION_USER
        | SQLValueFunctionOp::SVFOP_CURRENT_CATALOG
        | SQLValueFunctionOp::SVFOP_CURRENT_SCHEMA => {
            (*svf).r#type = NAMEOID;
        }
        #[allow(unreachable_patterns)]
        _ => {}
    }

    svf as *mut Node
}

unsafe fn transformXmlExpr(pstate: *mut ParseState, x: *mut ParseXmlExpr) -> *mut Node {
    let newx = makeNode!(XmlExpr, T_XmlExpr) as *mut XmlExpr;
    let mut i: c_int;

    (*newx).op = (*x).op;
    if !(*x).name.is_null() {
        (*newx).name = map_sql_identifier_to_xml_name((*x).name, false, false);
    } else {
        (*newx).name = std::ptr::null_mut();
    }
    (*newx).xmloption = (*x).xmloption;
    (*newx).r#type = XMLOID; /* this just marks the node as transformed */
    (*newx).typmod = -1;
    (*newx).location = (*x).location;

    /*
     * gram.y built the named args as a list of ResTarget.  Transform each,
     * and break the names out as a separate list.
     */
    (*newx).named_args = NIL;
    (*newx).arg_names = NIL;

    foreach!(lc, (*x).named_args, {
        let r = lfirst_node!(ResTarget, T_ResTarget, current_cell!(lc)) as *mut ResTarget;
        let expr: *mut Node;
        let argname: *mut c_char;

        expr = transformExprRecurse(pstate, (*r).val);

        if !(*r).name.is_null() {
            argname = map_sql_identifier_to_xml_name((*r).name, false, false);
        } else if IsA!((*r).val, T_ColumnRef) {
            argname = map_sql_identifier_to_xml_name(
                FigureColname((*r).val),
                true,
                false,
            );
        } else {
            ereport!(
                ERROR,
                if (*x).op == IS_XMLELEMENT {
                     errmsg!("unnamed XML attribute value must be a column reference")
                 } else {
                     errmsg!("unnamed XML element value must be a column reference")
                 }
                /* C also: errcode, errdetail, errhint, parser_errposition */
            );
            argname = std::ptr::null_mut(); /* keep compiler quiet */
        }

        /* reject duplicate argnames in XMLELEMENT only */
        if (*x).op == IS_XMLELEMENT {
            foreach!(lc2, (*newx).arg_names, {
                if strcmp(argname, strVal!(lfirst(current_cell!(lc2)) as *mut Node)) == 0 {
                    ereport!(
                        ERROR,
                        errmsg!("XML attribute name \"{}\" appears more than once", cstr_to_str(argname))
                        /* C also: errcode, parser_errposition */
                    );
                }
            });
        }

        (*newx).named_args = lappend((*newx).named_args, expr as *mut c_void);
        (*newx).arg_names = lappend((*newx).arg_names, makeString(argname) as *mut c_void);
    });

    /* The other arguments are of varying types depending on the function */
    (*newx).args = NIL;
    i = 0;
    foreach!(lc, (*x).args, {
        let e = lfirst(current_cell!(lc)) as *mut Node;
        let mut newe = transformExprRecurse(pstate, e);

        match (*x).op {
            IS_XMLCONCAT => {
                newe = coerce_to_specific_type(pstate, newe, XMLOID, cstr!("XMLCONCAT"));
            }
            IS_XMLELEMENT => {
                /* no coercion necessary */
            }
            IS_XMLFOREST => {
                newe = coerce_to_specific_type(pstate, newe, XMLOID, cstr!("XMLFOREST"));
            }
            IS_XMLPARSE => {
                if i == 0 {
                    newe = coerce_to_specific_type(pstate, newe, TEXTOID, cstr!("XMLPARSE"));
                } else {
                    newe = coerce_to_boolean(pstate, newe, cstr!("XMLPARSE"));
                }
            }
            IS_XMLPI => {
                newe = coerce_to_specific_type(pstate, newe, TEXTOID, cstr!("XMLPI"));
            }
            IS_XMLROOT => {
                if i == 0 {
                    newe = coerce_to_specific_type(pstate, newe, XMLOID, cstr!("XMLROOT"));
                } else if i == 1 {
                    newe = coerce_to_specific_type(pstate, newe, TEXTOID, cstr!("XMLROOT"));
                } else {
                    newe = coerce_to_specific_type(pstate, newe, INT4OID, cstr!("XMLROOT"));
                }
            }
            IS_XMLSERIALIZE => {
                /* not handled here */
                Assert!(false);
            }
            IS_DOCUMENT => {
                newe = coerce_to_specific_type(pstate, newe, XMLOID, cstr!("IS DOCUMENT"));
            }
            #[allow(unreachable_patterns)]
            _ => {}
        }
        (*newx).args = lappend((*newx).args, newe as *mut c_void);
        i += 1;
    });

    newx as *mut Node
}

unsafe fn transformXmlSerialize(pstate: *mut ParseState, xs: *mut XmlSerialize) -> *mut Node {
    let result: *mut Node;
    let xexpr = makeNode!(XmlExpr, T_XmlExpr) as *mut XmlExpr;
    let mut target_type: Oid = 0;
    let mut target_typmod: int32 = 0;

    (*xexpr).op = IS_XMLSERIALIZE;
    (*xexpr).args = list_make1!(coerce_to_specific_type(
        pstate,
        transformExprRecurse(pstate, (*xs).expr),
        XMLOID,
        cstr!("XMLSERIALIZE"),
    ));

    typenameTypeIdAndMod(pstate, (*xs).typeName, &mut target_type, &mut target_typmod);

    (*xexpr).xmloption = (*xs).xmloption;
    (*xexpr).indent = (*xs).indent;
    (*xexpr).location = (*xs).location;
    /* We actually only need these to be able to parse back the expression. */
    (*xexpr).r#type = target_type;
    (*xexpr).typmod = target_typmod;

    /*
     * The actual target type is determined this way.  SQL allows char and
     * varchar as target types.  We allow anything that can be cast implicitly
     * from text.  This way, user-defined text-like data types automatically
     * fit in.
     */
    result = coerce_to_target_type(
        pstate,
        xexpr as *mut Node,
        TEXTOID,
        target_type,
        target_typmod,
        COERCION_IMPLICIT,
        COERCE_IMPLICIT_CAST,
        -1,
    );
    if result.is_null() {
        ereport!(
            ERROR,
            errmsg!("cannot cast XMLSERIALIZE result to {}",
                     cstr_to_str(format_type_be(target_type)))
            /* C also: errcode, errdetail, errhint, parser_errposition */
        );
    }
    result
}

unsafe fn transformBooleanTest(
    pstate: *mut ParseState,
    b: *mut ParseBooleanTest,
) -> *mut Node {
    let clausename: *const c_char;

    #[allow(unreachable_patterns)]
    match (*b).booltesttype {
        BoolTestType::IS_TRUE => {
            clausename = cstr!("IS TRUE");
        }
        BoolTestType::IS_NOT_TRUE => {
            clausename = cstr!("IS NOT TRUE");
        }
        BoolTestType::IS_FALSE => {
            clausename = cstr!("IS FALSE");
        }
        BoolTestType::IS_NOT_FALSE => {
            clausename = cstr!("IS NOT FALSE");
        }
        BoolTestType::IS_UNKNOWN => {
            clausename = cstr!("IS UNKNOWN");
        }
        BoolTestType::IS_NOT_UNKNOWN => {
            clausename = cstr!("IS NOT UNKNOWN");
        }
        _ => {
            elog!(ERROR, "unrecognized booltesttype: {}", (*b).booltesttype as c_int);
            clausename = std::ptr::null(); /* keep compiler quiet */
        }
    }

    (*b).arg = transformExprRecurse(pstate, (*b).arg as *mut Node) as *mut Expr;

    (*b).arg = coerce_to_boolean(pstate, (*b).arg as *mut Node, clausename) as *mut Expr;

    b as *mut Node
}

unsafe fn transformCurrentOfExpr(
    pstate: *mut ParseState,
    cexpr: *mut CurrentOfExpr,
) -> *mut Node {
    /* CURRENT OF can only appear at top level of UPDATE/DELETE */
    Assert!(!(*pstate).p_target_nsitem.is_null());
    (*cexpr).cvarno = (*(*pstate).p_target_nsitem).p_rtindex as u32;

    /*
     * Check to see if the cursor name matches a parameter of type REFCURSOR.
     * If so, replace the raw name reference with a parameter reference. (This
     * is a hack for the convenience of plpgsql.)
     */
    if !(*cexpr).cursor_name.is_null() {
        /* in case already transformed */
        let cref = makeNode!(ColumnRef, T_ColumnRef) as *mut ColumnRef;
        let mut node: *mut Node = std::ptr::null_mut();

        /* Build an unqualified ColumnRef with the given name */
        (*cref).fields = list_make1!(makeString((*cexpr).cursor_name));
        (*cref).location = -1;

        /* See if there is a translation available from a parser hook */
        if let Some(hook) = (*pstate).p_pre_columnref_hook {
            node = hook(pstate, cref as *mut c_void);
        }
        if node.is_null() {
            if let Some(hook) = (*pstate).p_post_columnref_hook {
                node = hook(pstate, cref as *mut c_void, std::ptr::null_mut());
            }
        }

        /*
         * XXX Should we throw an error if we get a translation that isn't a
         * refcursor Param?  For now it seems best to silently ignore false
         * matches.
         */
        if !node.is_null() && IsA!(node, T_Param) {
            let p = node as *mut Param;

            if (*p).paramkind == ParamKind::PARAM_EXTERN && (*p).paramtype == REFCURSOROID {
                /* Matches, so convert CURRENT OF to a param reference */
                (*cexpr).cursor_name = std::ptr::null_mut();
                (*cexpr).cursor_param = (*p).paramid;
            }
        }
    }

    cexpr as *mut Node
}

/*
 * Construct a whole-row reference to represent the notation "relation.*".
 */
unsafe fn transformWholeRowRef(
    pstate: *mut ParseState,
    nsitem: *mut ParseNamespaceItem,
    sublevels_up: c_int,
    location: c_int,
) -> *mut Node {
    /*
     * Build the appropriate referencing node.  Normally this can be a
     * whole-row Var, but if the nsitem is a JOIN USING alias then it contains
     * only a subset of the columns of the underlying join RTE, so that will
     * not work.  Instead we immediately expand the reference into a RowExpr.
     * Since the JOIN USING's common columns are fully determined at this
     * point, there seems no harm in expanding it now rather than during
     * planning.
     *
     * Note that if the nsitem is an OLD/NEW alias for the target RTE (as can
     * appear in a RETURNING list), its alias won't match the target RTE's
     * alias, but we still want to make a whole-row Var here rather than a
     * RowExpr, for consistency with direct references to the target RTE, and
     * so that any dropped columns are handled correctly.  Thus we also check
     * p_returning_type here.
     *
     * Note that if the RTE is a function returning scalar, we create just a
     * plain reference to the function value, not a composite containing a
     * single column.  This is pretty inconsistent at first sight, but it's
     * what we've done historically.  One argument for it is that "rel" and
     * "rel.*" mean the same thing for composite relations, so why not for
     * scalar functions...
     */
    if (*nsitem).p_names == (*((*nsitem).p_rte as *mut crate::nodes::parsenodes::RangeTblEntry)).eref as *mut c_void
        || (*nsitem).p_returning_type != 0 /* VAR_RETURNING_DEFAULT */
    {
        let result = makeWholeRowVar(
            (*nsitem).p_rte as *mut crate::nodes::parsenodes::RangeTblEntry,
            (*nsitem).p_rtindex,
            sublevels_up,
            true,
        );

        /* mark Var for RETURNING OLD/NEW, as necessary */
        (*result).varreturningtype = std::mem::transmute::<i32, crate::nodes::primnodes::VarReturningType>((*nsitem).p_returning_type);

        /* location is not filled in by makeWholeRowVar */
        (*result).location = location;

        /* mark Var if it's nulled by any outer joins */
        markNullableIfNeeded(pstate, result);

        /* mark relation as requiring whole-row SELECT access */
        markVarForSelectPriv(pstate, result);

        result as *mut Node
    } else {
        let rowexpr = makeNode!(RowExpr, T_RowExpr) as *mut RowExpr;
        let mut fields: *mut List = NIL;

        /*
         * We want only as many columns as are listed in p_names->colnames,
         * and we should use those names not whatever possibly-aliased names
         * are in the RTE.  We needn't worry about marking the RTE for SELECT
         * access, as the common columns are surely so marked already.
         */
        expandRTE(
            (*nsitem).p_rte as *mut crate::nodes::parsenodes::RangeTblEntry,
            (*nsitem).p_rtindex,
            sublevels_up,
            (*nsitem).p_returning_type as c_int,
            location,
            false,
            std::ptr::null_mut(),
            &mut fields,
        );
        let p_names_alias = (*nsitem).p_names as *mut crate::nodes::primnodes::Alias;
        (*rowexpr).args = list_truncate(
            fields,
            list_length((*p_names_alias).colnames),
        );
        (*rowexpr).row_typeid = RECORDOID;
        (*rowexpr).row_format = CoercionForm::COERCE_IMPLICIT_CAST;
        (*rowexpr).colnames = copyObject((*p_names_alias).colnames);
        (*rowexpr).location = location;

        /* XXX we ought to mark the row as possibly nullable */

        rowexpr as *mut Node
    }
}

/*
 * Handle an explicit CAST construct.
 *
 * Transform the argument, look up the type name, and apply any necessary
 * coercion function(s).
 */
unsafe fn transformTypeCast(pstate: *mut ParseState, tc: *mut TypeCast) -> *mut Node {
    let result: *mut Node;
    let arg = (*tc).arg;
    let expr: *mut Node;
    let input_type: Oid;
    let mut target_type: Oid = 0;
    let mut target_typmod: int32 = 0;
    let location: c_int;

    /* Look up the type name first */
    typenameTypeIdAndMod(pstate, (*tc).typeName, &mut target_type, &mut target_typmod);

    /*
     * If the subject of the typecast is an ARRAY[] construct and the target
     * type is an array type, we invoke transformArrayExpr() directly so that
     * we can pass down the type information.  This avoids some cases where
     * transformArrayExpr() might not infer the correct type.  Otherwise, just
     * transform the argument normally.
     */
    if IsA!(arg, T_A_ArrayExpr) {
        let target_base_type: Oid;
        let mut target_base_typmod: int32;
        let element_type: Oid;

        /*
         * If target is a domain over array, work with the base array type
         * here.  Below, we'll cast the array type to the domain.  In the
         * usual case that the target is not a domain, the remaining steps
         * will be a no-op.
         */
        target_base_typmod = target_typmod;
        target_base_type = getBaseTypeAndTypmod(target_type, &mut target_base_typmod);
        element_type = get_element_type(target_base_type);
        if OidIsValid(element_type) {
            expr = transformArrayExpr(
                pstate,
                arg as *mut A_ArrayExpr,
                target_base_type,
                element_type,
                target_base_typmod,
            );
        } else {
            expr = transformExprRecurse(pstate, arg);
        }
    } else {
        expr = transformExprRecurse(pstate, arg);
    }

    input_type = exprType(expr);
    if input_type == InvalidOid!() {
        return expr; /* do nothing if NULL input */
    }

    /*
     * Location of the coercion is preferentially the location of the :: or
     * CAST symbol, but if there is none then use the location of the type
     * name (this can happen in TypeName 'string' syntax, for instance).
     */
    location = (*tc).location;
    let location = if location < 0 {
        (*(*tc).typeName).location
    } else {
        location
    };

    result = coerce_to_target_type(
        pstate,
        expr,
        input_type,
        target_type,
        target_typmod,
        COERCION_EXPLICIT,
        COERCE_EXPLICIT_CAST,
        location,
    );
    if result.is_null() {
        ereport!(
            ERROR,
            errmsg!("cannot cast type {} to {}",
                     cstr_to_str(format_type_be(input_type)),
                     cstr_to_str(format_type_be(target_type)))
            /* C also: errcode, errdetail, errhint, parser_errposition */
        );
    }

    result
}

/*
 * Handle an explicit COLLATE clause.
 *
 * Transform the argument, and look up the collation name.
 */
unsafe fn transformCollateClause(pstate: *mut ParseState, c: *mut CollateClause) -> *mut Node {
    let newc = makeNode!(CollateExpr, T_CollateExpr) as *mut CollateExpr;
    let argtype: Oid;

    (*newc).arg = transformExprRecurse(pstate, (*c).arg) as *mut Expr;

    argtype = exprType((*newc).arg as *const Node);

    /*
     * The unknown type is not collatable, but coerce_type() takes care of it
     * separately, so we'll let it go here.
     */
    if !type_is_collatable(argtype) && argtype != UNKNOWNOID {
        ereport!(
            ERROR,
            errmsg!("collations are not supported by type {}",
                     cstr_to_str(format_type_be(argtype)))
            /* C also: errcode, errdetail, errhint, parser_errposition */
        );
    }

    (*newc).collOid = LookupCollation(pstate, (*c).collname, (*c).location);
    (*newc).location = (*c).location;

    newc as *mut Node
}

/*
 * Transform a "row compare-op row" construct
 *
 * The inputs are lists of already-transformed expressions.
 * As with coerce_type, pstate may be NULL if no special unknown-Param
 * processing is wanted.
 *
 * The output may be a single OpExpr, an AND or OR combination of OpExprs,
 * or a RowCompareExpr.  In all cases it is guaranteed to return boolean.
 * The AND, OR, and RowCompareExpr cases further imply things about the
 * behavior of the operators (ie, they behave as =, <>, or < <= > >=).
 */
unsafe fn make_row_comparison_op(
    pstate: *mut ParseState,
    opname: *mut List,
    mut largs: *mut List,
    mut rargs: *mut List,
    location: c_int,
) -> *mut Node {
    let rcexpr: *mut RowCompareExpr;
    let cmptype: CompareType;
    let mut opexprs: *mut List = NIL;
    let mut opnos: *mut List = NIL;
    let mut opfamilies: *mut List = NIL;
    let nopers: c_int;
    let mut i: c_int;

    nopers = list_length(largs);
    if nopers != list_length(rargs) {
        ereport!(
            ERROR,
            errmsg!("unequal number of entries in row expressions")
            /* C also: errcode, errdetail, errhint, parser_errposition */
        );
    }

    /*
     * We can't compare zero-length rows because there is no principled basis
     * for figuring out what the operator is.
     */
    if nopers == 0 {
        ereport!(
            ERROR,
            errmsg!("cannot compare rows of zero length")
            /* C also: errcode, errdetail, errhint, parser_errposition */
        );
    }

    /*
     * Identify all the pairwise operators, using make_op so that behavior is
     * the same as in the simple scalar case.
     */
    forboth!(l_cell, largs, r_cell, rargs, {
        let larg = lfirst(l_cell) as *mut Node;
        let rarg = lfirst(r_cell) as *mut Node;

        let cmp = castNode!(OpExpr, T_OpExpr, make_op(pstate, opname, larg, rarg, (*pstate).p_last_srf, location)
        ) as *mut OpExpr;

        /*
         * We don't use coerce_to_boolean here because we insist on the
         * operator yielding boolean directly, not via coercion.  If it
         * doesn't yield bool it won't be in any index opfamilies...
         */
        if (*cmp).opresulttype != BOOLOID {
            ereport!(
                ERROR,
                errmsg!("row comparison operator must yield type boolean, not type {}",
                        cstr_to_str(format_type_be((*cmp).opresulttype)))
                /* C also: errcode, parser_errposition */
            );
        }
        if expression_returns_set(cmp as *mut Node) {
            ereport!(
                ERROR,
                errmsg!("row comparison operator must not return a set")
                /* C also: errcode, parser_errposition */
            );
        }
        opexprs = lappend(opexprs, cmp as *mut c_void);
    });

    /*
     * If rows are length 1, just return the single operator.  In this case we
     * don't insist on identifying btree semantics for the operator (but we
     * still require it to return boolean).
     */
    if nopers == 1 {
        return linitial(opexprs) as *mut Node;
    }

    /*
     * Now we must determine which row comparison semantics (= <> < <= > >=)
     * apply to this set of operators.  We look for opfamilies containing the
     * operators, and see which interpretations (cmptypes) exist for each
     * operator.
     */
    let opinfo_lists = palloc(
        (nopers as usize) * std::mem::size_of::<*mut List>(),
    ) as *mut *mut List;
    let mut cmptypes: *mut Bitmapset = std::ptr::null_mut();
    i = 0;
    foreach!(l_cell, opexprs, {
        let opno = (*(lfirst(current_cell!(l_cell)) as *mut OpExpr)).opno;
        let mut this_cmptypes: *mut Bitmapset = std::ptr::null_mut();

        *opinfo_lists.offset(i as isize) = get_op_index_interpretation(opno) as *mut List;

        /*
         * convert comparison types into a Bitmapset to make the intersection
         * calculation easy.
         */
        foreach!(j_cell, *opinfo_lists.offset(i as isize), {
            let opinfo = lfirst(current_cell!(j_cell)) as *mut crate::utils::cache::lsyscache::OpIndexInterpretation;
            this_cmptypes = bms_add_member(this_cmptypes, (*opinfo).cmptype as c_int);
        });
        if i == 0 {
            cmptypes = this_cmptypes;
        } else {
            cmptypes = bms_int_members(cmptypes, this_cmptypes);
        }
        i += 1;
    });

    /*
     * If there are multiple common interpretations, we may use any one of
     * them ... this coding arbitrarily picks the lowest comparison type
     * number.
     */
    i = bms_next_member(cmptypes, -1);
    if i < 0 {
        /* No common interpretation, so fail */
        ereport!(
            ERROR,
            errmsg!("could not determine interpretation of row comparison operator {}",
                     cstr_to_str(strVal!(llast(opname) as *mut Node)))
            /* C also: errcode, errdetail, errhint, parser_errposition */
        );
    }
    cmptype = i as CompareType;

    /*
     * For = and <> cases, we just combine the pairwise operators with AND or
     * OR respectively.
     */
    if cmptype == crate::access::cmptype::COMPARE_EQ {
        return makeBoolExpr(BoolExprType::AND_EXPR, opexprs, location) as *mut Node;
    }
    if cmptype == crate::access::cmptype::COMPARE_NE {
        return makeBoolExpr(BoolExprType::OR_EXPR, opexprs, location) as *mut Node;
    }

    /*
     * Otherwise we need to choose exactly which opfamily to associate with
     * each operator.
     */
    opfamilies = NIL;
    for k in 0..nopers {
        let mut opfamily: Oid = InvalidOid!();

        foreach!(j_cell, *opinfo_lists.offset(k as isize), {
            let opinfo = lfirst(current_cell!(j_cell)) as *mut crate::utils::cache::lsyscache::OpIndexInterpretation;
            if (*opinfo).cmptype == cmptype {
                opfamily = (*opinfo).opfamily_id;
                break;
            }
        });
        if OidIsValid(opfamily) {
            opfamilies = lappend_oid(opfamilies, opfamily);
        } else {
            /* should not happen */
            ereport!(
                ERROR,
                errmsg!("could not determine interpretation of row comparison operator {}",
                         cstr_to_str(strVal!(llast(opname) as *mut Node)))
                /* C also: errcode, errdetail, errhint, parser_errposition */
            );
        }
    }

    /*
     * Now deconstruct the OpExprs and create a RowCompareExpr.
     *
     * Note: can't just reuse the passed largs/rargs lists, because of
     * possibility that make_op inserted coercion operations.
     */
    opnos = NIL;
    largs = NIL;
    rargs = NIL;
    foreach!(l_cell, opexprs, {
        let cmp = lfirst(current_cell!(l_cell)) as *mut OpExpr;

        opnos = lappend_oid(opnos, (*cmp).opno);
        largs = lappend(largs, linitial((*cmp).args) as *mut c_void);
        rargs = lappend(rargs, lsecond((*cmp).args) as *mut c_void);
    });

    rcexpr = makeNode!(RowCompareExpr, T_RowCompareExpr) as *mut RowCompareExpr;
    (*rcexpr).cmptype = cmptype;
    (*rcexpr).opnos = opnos;
    (*rcexpr).opfamilies = opfamilies;
    (*rcexpr).inputcollids = NIL; /* assign_expr_collations will fix this */
    (*rcexpr).largs = largs;
    (*rcexpr).rargs = rargs;

    rcexpr as *mut Node
}

/*
 * Transform a "row IS DISTINCT FROM row" construct
 *
 * The input RowExprs are already transformed
 */
unsafe fn make_row_distinct_op(
    pstate: *mut ParseState,
    opname: *mut List,
    lrow: *mut RowExpr,
    rrow: *mut RowExpr,
    location: c_int,
) -> *mut Node {
    let mut result: *mut Node = std::ptr::null_mut();
    let largs = (*lrow).args;
    let rargs = (*rrow).args;

    if list_length(largs) != list_length(rargs) {
        ereport!(
            ERROR,
            errmsg!("unequal number of entries in row expressions")
            /* C also: errcode, errdetail, errhint, parser_errposition */
        );
    }

    forboth!(l_cell, largs, r_cell, rargs, {
        let larg = lfirst(l_cell) as *mut Node;
        let rarg = lfirst(r_cell) as *mut Node;

        let cmp = make_distinct_op(pstate, opname, larg, rarg, location) as *mut Node;
        if result.is_null() {
            result = cmp;
        } else {
            result = makeBoolExpr(
                BoolExprType::OR_EXPR,
                list_make2!(result, cmp),
                location,
            ) as *mut Node;
        }
    });

    if result.is_null() {
        /* zero-length rows?  Generate constant FALSE */
        result = makeBoolConst(false, false);
    }

    result
}

/*
 * make the node for an IS DISTINCT FROM operator
 */
unsafe fn make_distinct_op(
    pstate: *mut ParseState,
    opname: *mut List,
    ltree: *mut Node,
    rtree: *mut Node,
    location: c_int,
) -> *mut Expr {
    let result = make_op(pstate, opname, ltree, rtree, (*pstate).p_last_srf, location);

    if (*(result as *mut OpExpr)).opresulttype != BOOLOID {
        ereport!(
            ERROR,
            errmsg!("set-returning functions are not allowed in this context")
            /* C also: errcode, parser_errposition */
        );
    }
    if (*(result as *mut OpExpr)).opretset {
        ereport!(
            ERROR,
            errmsg!("set-returning functions are not allowed in this context")
            /* C also: errcode, parser_errposition */
        );
    }

    /*
     * We rely on DistinctExpr and OpExpr being same struct
     */
    NodeSetTag!(result, T_DistinctExpr);

    result as *mut Expr
}

/*
 * Produce a NullTest node from an IS [NOT] DISTINCT FROM NULL construct
 *
 * "arg" is the untransformed other argument
 */
unsafe fn make_nulltest_from_distinct(
    pstate: *mut ParseState,
    distincta: *mut A_Expr,
    arg: *mut Node,
) -> *mut Node {
    let nt = makeNode!(NullTest, T_NullTest) as *mut NullTest;

    (*nt).arg = transformExprRecurse(pstate, arg) as *mut Expr;
    /* the argument can be any type, so don't coerce it */
    if (*distincta).kind == A_ExprKind::AEXPR_NOT_DISTINCT {
        (*nt).nulltesttype = NullTestType::IS_NULL;
    } else {
        (*nt).nulltesttype = NullTestType::IS_NOT_NULL;
    }
    /* argisrow = false is correct whether or not arg is composite */
    (*nt).argisrow = false;
    (*nt).location = (*distincta).location;
    nt as *mut Node
}

/*
 * Produce a string identifying an expression by kind.
 *
 * Note: when practical, use a simple SQL keyword for the result.  If that
 * doesn't work well, check call sites to see whether custom error message
 * strings are required.
 */
pub unsafe fn ParseExprKindName(expr_kind: ParseExprKind) -> *const c_char {
    match expr_kind {
        EXPR_KIND_NONE => cstr!("invalid expression context"),
        EXPR_KIND_OTHER => cstr!("extension expression"),
        EXPR_KIND_JOIN_ON => cstr!("JOIN/ON"),
        EXPR_KIND_JOIN_USING => cstr!("JOIN/USING"),
        EXPR_KIND_FROM_SUBSELECT => cstr!("sub-SELECT in FROM"),
        EXPR_KIND_FROM_FUNCTION => cstr!("function in FROM"),
        EXPR_KIND_WHERE => cstr!("WHERE"),
        EXPR_KIND_POLICY => cstr!("POLICY"),
        EXPR_KIND_HAVING => cstr!("HAVING"),
        EXPR_KIND_FILTER => cstr!("FILTER"),
        EXPR_KIND_WINDOW_PARTITION => cstr!("window PARTITION BY"),
        EXPR_KIND_WINDOW_ORDER => cstr!("window ORDER BY"),
        EXPR_KIND_WINDOW_FRAME_RANGE => cstr!("window RANGE"),
        EXPR_KIND_WINDOW_FRAME_ROWS => cstr!("window ROWS"),
        EXPR_KIND_WINDOW_FRAME_GROUPS => cstr!("window GROUPS"),
        EXPR_KIND_SELECT_TARGET => cstr!("SELECT"),
        EXPR_KIND_INSERT_TARGET => cstr!("INSERT"),
        EXPR_KIND_UPDATE_SOURCE | EXPR_KIND_UPDATE_TARGET => cstr!("UPDATE"),
        EXPR_KIND_MERGE_WHEN => cstr!("MERGE WHEN"),
        EXPR_KIND_GROUP_BY => cstr!("GROUP BY"),
        EXPR_KIND_ORDER_BY => cstr!("ORDER BY"),
        EXPR_KIND_DISTINCT_ON => cstr!("DISTINCT ON"),
        EXPR_KIND_LIMIT => cstr!("LIMIT"),
        EXPR_KIND_OFFSET => cstr!("OFFSET"),
        EXPR_KIND_RETURNING | EXPR_KIND_MERGE_RETURNING => cstr!("RETURNING"),
        EXPR_KIND_VALUES | EXPR_KIND_VALUES_SINGLE => cstr!("VALUES"),
        EXPR_KIND_CHECK_CONSTRAINT | EXPR_KIND_DOMAIN_CHECK => cstr!("CHECK"),
        EXPR_KIND_COLUMN_DEFAULT | EXPR_KIND_FUNCTION_DEFAULT => cstr!("DEFAULT"),
        EXPR_KIND_INDEX_EXPRESSION => cstr!("index expression"),
        EXPR_KIND_INDEX_PREDICATE => cstr!("index predicate"),
        EXPR_KIND_STATS_EXPRESSION => cstr!("statistics expression"),
        EXPR_KIND_ALTER_COL_TRANSFORM => cstr!("USING"),
        EXPR_KIND_EXECUTE_PARAMETER => cstr!("EXECUTE"),
        EXPR_KIND_TRIGGER_WHEN => cstr!("WHEN"),
        EXPR_KIND_PARTITION_BOUND => cstr!("partition bound"),
        EXPR_KIND_PARTITION_EXPRESSION => cstr!("PARTITION BY"),
        EXPR_KIND_CALL_ARGUMENT => cstr!("CALL"),
        EXPR_KIND_COPY_WHERE => cstr!("WHERE"),
        EXPR_KIND_GENERATED_COLUMN => cstr!("GENERATED AS"),
        EXPR_KIND_CYCLE_MARK => cstr!("CYCLE"),
        /*
         * There is intentionally no default: case here, so that the
         * compiler will warn if we add a new ParseExprKind without
         * extending this switch.  If we do see an unrecognized value at
         * runtime, we'll fall through to the "unrecognized" return.
         */
        #[allow(unreachable_patterns)]
        _ => cstr!("unrecognized expression kind"),
    }
}

/*
 * Make string Const node from JSON encoding name.
 *
 * UTF8 is default encoding.
 */
unsafe fn getJsonEncodingConst(format: *mut JsonFormat) -> *mut crate::nodes::primnodes::Const {
    let encoding: JsonEncoding;
    let enc: *const c_char;
    let encname = palloc(NAMEDATALEN) as *mut crate::c::NameData;

    if format.is_null()
        || (*format).format_type == JS_FORMAT_DEFAULT
        || (*format).encoding == JS_ENC_DEFAULT
    {
        encoding = JS_ENC_UTF8;
    } else {
        encoding = (*format).encoding;
    }

    #[allow(unreachable_patterns)]
    match encoding {
        JS_ENC_UTF16 => {
            enc = cstr!("UTF16");
        }
        JS_ENC_UTF32 => {
            enc = cstr!("UTF32");
        }
        JS_ENC_UTF8 | JS_ENC_DEFAULT => {
            enc = cstr!("UTF8");
        }
        _ => {
            elog!(ERROR, "invalid JSON encoding: {}", encoding as c_int);
            enc = std::ptr::null(); /* keep compiler quiet */
        }
    }

    crate::utils::builtins::namestrcpy(encname, enc);

    makeConst(
        NAMEOID,
        -1,
        InvalidOid!(),
        NAMEDATALEN as i32,
        crate::postgres::Datum::from(encname as usize),
        false,
        false,
    )
}

/*
 * Make bytea => text conversion using specified JSON format encoding.
 */
unsafe fn makeJsonByteaToTextConversion(
    expr: *mut Node,
    format: *mut JsonFormat,
    location: c_int,
) -> *mut Node {
    let encoding = getJsonEncodingConst(format);
    let fexpr = makeFuncExpr(
        F_CONVERT_FROM,
        TEXTOID,
        list_make2!(expr, encoding),
        InvalidOid!(),
        InvalidOid!(),
        CoercionForm::COERCE_EXPLICIT_CALL,
    );

    (*fexpr).location = location;

    fexpr as *mut Node
}

/*
 * Transform JSON value expression using specified input JSON format or
 * default format otherwise, coercing to the targettype if needed.
 *
 * Returned expression is either ve->raw_expr coerced to text (if needed) or
 * a JsonValueExpr with formatted_expr set to the coerced copy of raw_expr
 * if the specified format and the targettype requires it.
 */
unsafe fn transformJsonValueExpr(
    pstate: *mut ParseState,
    construct_name: *const c_char,
    ve: *mut ParseJsonValueExpr,
    default_format: JsonFormatType,
    mut targettype: Oid,
    isarg: bool,
) -> *mut Node {
    let mut expr = transformExprRecurse(pstate, (*ve).raw_expr as *mut Node);
    let rawexpr: *mut Node;
    let format: JsonFormatType;
    let mut exprtype: Oid;
    let location: c_int;
    let mut typcategory: c_char = 0;
    let mut typispreferred: bool = false;

    if exprType(expr) == UNKNOWNOID {
        expr = coerce_to_specific_type(pstate, expr, TEXTOID, construct_name);
    }

    rawexpr = expr;
    exprtype = exprType(expr);
    location = exprLocation(expr);

    get_type_category_preferred(exprtype, &mut typcategory, &mut typispreferred);

    if (*(*ve).format).format_type != JS_FORMAT_DEFAULT {
        if (*(*ve).format).encoding != JS_ENC_DEFAULT && exprtype != BYTEAOID {
            ereport!(ERROR, errmsg!("JSON ENCODING clause is only allowed for bytea input type")) /* C also: errcode, parser_errposition */;
        }

        if exprtype == JSONOID || exprtype == JSONBOID {
            format = JS_FORMAT_DEFAULT; /* do not format json[b] types */
        } else {
            format = (*(*ve).format).format_type;
        }
    } else if isarg {
        /*
         * Special treatment for PASSING arguments.
         *
         * Pass types supported by GetJsonPathVar() / JsonItemFromDatum()
         * directly without converting to json[b].
         */
        match exprtype {
            t if t == BOOLOID || t == NUMERICOID || t == INT2OID || t == INT4OID
                || t == INT8OID || t == FLOAT4OID || t == FLOAT8OID
                || t == TEXTOID || t == VARCHAROID || t == DATEOID
                || t == TIMEOID || t == TIMETZOID || t == TIMESTAMPOID
                || t == TIMESTAMPTZOID =>
            {
                return expr;
            }
            _ => {
                if typcategory == TYPCATEGORY_STRING {
                    return expr;
                }
                /* else convert argument to json[b] type */
            }
        }
        format = default_format;
    } else if exprtype == JSONOID || exprtype == JSONBOID {
        format = JS_FORMAT_DEFAULT; /* do not format json[b] types */
    } else {
        format = default_format;
    }

    if format != JS_FORMAT_DEFAULT
        || (OidIsValid(targettype) && exprtype != targettype)
    {
        let coerced: *mut Node;
        let only_allow_cast = OidIsValid(targettype);

        /*
         * PASSING args are handled appropriately by GetJsonPathVar() /
         * JsonItemFromDatum().
         */
        if !isarg
            && !only_allow_cast
            && exprtype != BYTEAOID
            && typcategory != TYPCATEGORY_STRING
        {
            ereport!(
                ERROR,
                if (*(*ve).format).format_type == JS_FORMAT_DEFAULT {
                    errmsg!("cannot use non-string types with implicit FORMAT JSON clause")
                } else {
                    errmsg!("cannot use non-string types with explicit FORMAT JSON clause")
                }
                /* C also: errcode, parser_errposition */
            );
        }

        /* Convert encoded JSON text from bytea. */
        if format == JS_FORMAT_JSON && exprtype == BYTEAOID {
            expr = makeJsonByteaToTextConversion(expr, (*ve).format, location);
            exprtype = TEXTOID;
        }

        if !OidIsValid(targettype) {
            targettype = if format == JS_FORMAT_JSONB { JSONBOID } else { JSONOID };
        }

        /* Try to coerce to the target type. */
        let coerced = coerce_to_target_type(
            pstate, expr, exprtype, targettype, -1,
            COERCION_EXPLICIT, COERCE_EXPLICIT_CAST, location,
        );

        if coerced.is_null() {
            /* If coercion failed, use to_json()/to_jsonb() functions. */
            let fexpr: *mut FuncExpr;

            /*
             * Though only allow a cast when the target type is specified by
             * the caller.
             */
            if only_allow_cast {
                ereport!(
                    ERROR,
                    errmsg!("cannot cast type {} to {}",
                             cstr_to_str(format_type_be(exprtype)),
                             cstr_to_str(format_type_be(targettype)))
                    /* C also: errcode, errdetail, errhint, parser_errposition */
                );
            }

            let fnoid = if targettype == JSONOID { F_TO_JSON } else { F_TO_JSONB };
            let fe = makeFuncExpr(
                fnoid, targettype, list_make1!(expr),
                InvalidOid!(), InvalidOid!(), CoercionForm::COERCE_EXPLICIT_CALL,
            );

            (*fe).location = location;

            let coerced = fe as *mut Node;

            let ve_copy = copyObject(ve);
            (*ve_copy).raw_expr = rawexpr as *mut Expr;
            (*ve_copy).formatted_expr = coerced as *mut Expr;
            return ve_copy as *mut Node;
        } else if coerced == expr {
            expr = rawexpr;
        } else {
            let ve_copy = copyObject(ve);
            (*ve_copy).raw_expr = rawexpr as *mut Expr;
            (*ve_copy).formatted_expr = coerced as *mut Expr;
            expr = ve_copy as *mut Node;
        }
    }

    /* If returning a JsonValueExpr, formatted_expr must have been set. */
    Assert!(
        !IsA!(expr, T_JsonValueExpr)
            || !(*(expr as *mut crate::nodes::primnodes::JsonValueExpr)).formatted_expr.is_null()
    );

    expr
}

/*
 * Checks specified output format for its applicability to the target type.
 */
unsafe fn checkJsonOutputFormat(
    pstate: *mut ParseState,
    format: *const JsonFormat,
    targettype: Oid,
    allow_format_for_non_strings: bool,
) {
    if !allow_format_for_non_strings
        && (*format).format_type != JS_FORMAT_DEFAULT
        && targettype != BYTEAOID
        && targettype != JSONOID
        && targettype != JSONBOID
    {
        let mut typcategory: c_char = 0;
        let mut typispreferred: bool = false;

        get_type_category_preferred(targettype, &mut typcategory, &mut typispreferred);

        if typcategory != TYPCATEGORY_STRING {
            ereport!(ERROR, errmsg!("cannot use JSON format with non-string output types")) /* C also: errcode, parser_errposition */;
        }
    }

    if (*format).format_type == JS_FORMAT_JSON {
        let enc = if (*format).encoding != JS_ENC_DEFAULT {
            (*format).encoding
        } else {
            JS_ENC_UTF8
        };

        if targettype != BYTEAOID && (*format).encoding != JS_ENC_DEFAULT {
            ereport!(ERROR, errmsg!("cannot set JSON encoding for non-bytea output types")) /* C also: errcode, parser_errposition */;
        }

        if enc != JS_ENC_UTF8 {
            ereport!(ERROR, errmsg!("unsupported JSON encoding")) /* C also: errcode, errhint, parser_errposition */;
        }
    }
}

/*
 * Transform JSON output clause.
 *
 * Assigns target type oid and modifier.
 * Assigns default format or checks specified format for its applicability to
 * the target type.
 */
unsafe fn transformJsonOutput(
    pstate: *mut ParseState,
    output: *const JsonOutput,
    allow_format: bool,
) -> *mut JsonReturning {
    let ret: *mut JsonReturning;

    /* if output clause is not specified, make default clause value */
    if output.is_null() {
        ret = makeNode!(JsonReturning, T_JsonReturning) as *mut JsonReturning;

        (*ret).format = makeJsonFormat(JS_FORMAT_DEFAULT, JS_ENC_DEFAULT, -1);
        (*ret).typid = InvalidOid!();
        (*ret).typmod = -1;

        return ret;
    }

    ret = copyObject((*output).returning);

    typenameTypeIdAndMod(pstate, (*output).typeName, &mut (*ret).typid, &mut (*ret).typmod);

    if (*(*output).typeName).setof {
        ereport!(ERROR, errmsg!("returning SETOF types is not supported in SQL/JSON functions")) /* C also: errcode */;
    }

    if get_typtype((*ret).typid) == TYPTYPE_PSEUDO {
        ereport!(ERROR, errmsg!("returning pseudo-types is not supported in SQL/JSON functions")) /* C also: errcode */;
    }

    if (*(*ret).format).format_type == JS_FORMAT_DEFAULT {
        /* assign JSONB format when returning jsonb, or JSON format otherwise */
        (*(*ret).format).format_type = if (*ret).typid == JSONBOID {
            JS_FORMAT_JSONB
        } else {
            JS_FORMAT_JSON
        };
    } else {
        checkJsonOutputFormat(pstate, (*ret).format, (*ret).typid, allow_format);
    }

    ret
}

/*
 * Transform JSON output clause of JSON constructor functions.
 *
 * Derive RETURNING type, if not specified, from argument types.
 */
unsafe fn transformJsonConstructorOutput(
    pstate: *mut ParseState,
    output: *mut JsonOutput,
    args: *mut List,
) -> *mut JsonReturning {
    let returning = transformJsonOutput(pstate, output, true);

    if !OidIsValid((*returning).typid) {
        let mut have_jsonb = false;

        foreach!(lc, args, {
            let expr = lfirst(current_cell!(lc)) as *mut Node;
            let typid = exprType(expr);

            have_jsonb |= typid == JSONBOID;

            if have_jsonb {
                break;
            }
        });

        if have_jsonb {
            (*returning).typid = JSONBOID;
            (*(*returning).format).format_type = JS_FORMAT_JSONB;
        } else {
            /* XXX TEXT is default by the standard, but we return JSON */
            (*returning).typid = JSONOID;
            (*(*returning).format).format_type = JS_FORMAT_JSON;
        }

        (*returning).typmod = -1;
    }

    returning
}

/*
 * Coerce json[b]-valued function expression to the output type.
 */
unsafe fn coerceJsonFuncExpr(
    pstate: *mut ParseState,
    expr: *mut Node,
    returning: *const JsonReturning,
    report_error: bool,
) -> *mut Node {
    let res: *mut Node;
    let location: c_int;
    let exprtype = exprType(expr);

    /* if output type is not specified or equals to function type, return */
    if !OidIsValid((*returning).typid) || (*returning).typid == exprtype {
        return expr;
    }

    let loc = exprLocation(expr);
    location = if loc < 0 { (*(*returning).format).location } else { loc };

    /* special case for RETURNING bytea FORMAT json */
    if (*(*returning).format).format_type == JS_FORMAT_JSON
        && (*returning).typid == BYTEAOID
    {
        /* encode json text into bytea using pg_convert_to() */
        let texpr = coerce_to_specific_type(pstate, expr, TEXTOID, cstr!("JSON_FUNCTION"));
        let enc = getJsonEncodingConst((*returning).format);
        let fexpr = makeFuncExpr(
            F_CONVERT_TO,
            BYTEAOID,
            list_make2!(texpr, enc),
            InvalidOid!(),
            InvalidOid!(),
            CoercionForm::COERCE_EXPLICIT_CALL,
        );

        (*fexpr).location = location;

        return fexpr as *mut Node;
    }

    /*
     * For other cases, try to coerce expression to the output type using
     * assignment-level casts, erroring out if none available.  This basically
     * allows coercing the jsonb value to any string type (typcategory = 'S').
     *
     * Requesting assignment-level here means that typmod / length coercion
     * assumes implicit coercion which is the behavior we want; see
     * build_coercion_expression().
     */
    res = coerce_to_target_type(
        pstate,
        expr,
        exprtype,
        (*returning).typid,
        (*returning).typmod,
        COERCION_ASSIGNMENT,
        COERCE_IMPLICIT_CAST,
        location,
    );

    if res.is_null() && report_error {
        ereport!(ERROR, errmsg!("cannot cast type {} to {}",
                    cstr_to_str(format_type_be(exprtype)),
                    cstr_to_str(format_type_be((*returning).typid)))) /* C also: errcode, parser_coercion_errposition */;
    }

    res
}

/*
 * Make a JsonConstructorExpr node.
 */
unsafe fn makeJsonConstructorExpr(
    pstate: *mut ParseState,
    r#type: JsonConstructorType,
    args: *mut List,
    fexpr: *mut Expr,
    returning: *mut JsonReturning,
    unique: bool,
    absent_on_null: bool,
    location: c_int,
) -> *mut Node {
    let jsctor = makeNode!(JsonConstructorExpr, T_JsonConstructorExpr) as *mut JsonConstructorExpr;
    let placeholder: *mut Node;
    let coercion: *mut Node;

    (*jsctor).args = args;
    (*jsctor).func = fexpr;
    (*jsctor).r#type = r#type;
    (*jsctor).returning = returning;
    (*jsctor).unique = unique;
    (*jsctor).absent_on_null = absent_on_null;
    (*jsctor).location = location;

    /*
     * Coerce to the RETURNING type and format, if needed.  We abuse
     * CaseTestExpr here as placeholder to pass the result of either
     * evaluating 'fexpr' or whatever is produced by ExecEvalJsonConstructor()
     * that is of type JSON or JSONB to the coercion function.
     */
    if !fexpr.is_null() {
        let cte = makeNode!(CaseTestExpr, T_CaseTestExpr) as *mut CaseTestExpr;

        (*cte).typeId = exprType(fexpr as *const Node);
        (*cte).typeMod = exprTypmod(fexpr as *const Node);
        (*cte).collation = exprCollation(fexpr as *const Node);

        placeholder = cte as *mut Node;
    } else {
        let cte = makeNode!(CaseTestExpr, T_CaseTestExpr) as *mut CaseTestExpr;

        (*cte).typeId = if (*(*returning).format).format_type == JS_FORMAT_JSONB {
            JSONBOID
        } else {
            JSONOID
        };
        (*cte).typeMod = -1;
        (*cte).collation = InvalidOid!();

        placeholder = cte as *mut Node;
    }

    coercion = coerceJsonFuncExpr(pstate, placeholder, returning, true);

    if coercion != placeholder {
        (*jsctor).coercion = coercion as *mut Expr;
    }

    jsctor as *mut Node
}

/*
 * Transform JSON_OBJECT() constructor.
 *
 * JSON_OBJECT() is transformed into a JsonConstructorExpr node of type
 * JSCTOR_JSON_OBJECT.  The result is coerced to the target type given
 * by ctor->output.
 */
unsafe fn transformJsonObjectConstructor(
    pstate: *mut ParseState,
    ctor: *mut JsonObjectConstructor,
) -> *mut Node {
    let returning: *mut JsonReturning;
    let mut args: *mut List = NIL;

    /* transform key-value pairs, if any */
    if !(*ctor).exprs.is_null() {

        /* transform and append key-value arguments */
        foreach!(lc, (*ctor).exprs, {
            let kv = castNode!(JsonKeyValue, T_JsonKeyValue, lfirst(current_cell!(lc))) as *mut JsonKeyValue;
            let key = transformExprRecurse(pstate, (*kv).key as *mut Node);
            let val = transformJsonValueExpr(
                pstate,
                cstr!("JSON_OBJECT()"),
                (*kv).value,
                JS_FORMAT_DEFAULT,
                InvalidOid!(),
                false,
            );

            args = lappend(args, key as *mut c_void);
            args = lappend(args, val as *mut c_void);
        });
    }

    returning = transformJsonConstructorOutput(pstate, (*ctor).output, args);

    makeJsonConstructorExpr(
        pstate,
        JSCTOR_JSON_OBJECT,
        args,
        std::ptr::null_mut(),
        returning,
        (*ctor).unique,
        (*ctor).absent_on_null,
        (*ctor).location,
    )
}

/*
 * Transform JSON_ARRAY(query [FORMAT] [RETURNING] [ON NULL]) into
 *  (SELECT  JSON_ARRAYAGG(a  [FORMAT] [RETURNING] [ON NULL]) FROM (query) q(a))
 */
unsafe fn transformJsonArrayQueryConstructor(
    pstate: *mut ParseState,
    ctor: *mut JsonArrayQueryConstructor,
) -> *mut Node {
    let sublink = makeNode!(SubLink, T_SubLink) as *mut SubLink;
    let select = makeNode!(SelectStmt, T_SelectStmt) as *mut SelectStmt;
    let range = makeNode!(RangeSubselect, T_RangeSubselect) as *mut RangeSubselect;
    let alias = makeNode!(Alias, T_Alias) as *mut Alias;
    let target = makeNode!(ResTarget, T_ResTarget) as *mut ResTarget;
    let agg = makeNode!(JsonArrayAgg, T_JsonArrayAgg) as *mut JsonArrayAgg;
    let colref = makeNode!(ColumnRef, T_ColumnRef) as *mut ColumnRef;
    let query: *mut Query;
    let qpstate: *mut ParseState;

    /* Transform query only for counting target list entries. */
    qpstate = make_parsestate(pstate);

    query = transformStmt(qpstate, copyObject((*ctor).query) as *mut Node);

    if count_nonjunk_tlist_entries((*query).targetList) != 1 {
        ereport!(ERROR, errmsg!("subquery must return only one column")) /* C also: errcode, parser_errposition */;
    }

    free_parsestate(qpstate);

    (*colref).fields = list_make2!(
        makeString(pstrdup(cstr!("q"))),
        makeString(pstrdup(cstr!("a")))
    );
    (*colref).location = (*ctor).location;

    /*
     * No formatting necessary, so set formatted_expr to be the same as
     * raw_expr.
     */
    (*agg).arg = makeJsonValueExpr(
        colref as *mut Expr,
        colref as *mut Expr,
        (*ctor).format,
    );
    (*agg).absent_on_null = (*ctor).absent_on_null;
    (*agg).constructor = makeNode!(JsonAggConstructor, T_JsonAggConstructor) as *mut crate::nodes::parsenodes::JsonAggConstructor;
    (*(*agg).constructor).agg_order = NIL;
    (*(*agg).constructor).output = (*ctor).output;
    (*(*agg).constructor).location = (*ctor).location;

    (*target).name = std::ptr::null_mut();
    (*target).indirection = NIL;
    (*target).val = agg as *mut Node;
    (*target).location = (*ctor).location;

    (*alias).aliasname = pstrdup(cstr!("q"));
    (*alias).colnames = list_make1!(makeString(pstrdup(cstr!("a"))));

    (*range).lateral = false;
    (*range).subquery = (*ctor).query;
    (*range).alias = alias;

    (*select).targetList = list_make1!(target);
    (*select).fromClause = list_make1!(range);

    (*sublink).subLinkType = SubLinkType::EXPR_SUBLINK;
    (*sublink).subLinkId = 0;
    (*sublink).testexpr = std::ptr::null_mut();
    (*sublink).operName = NIL;
    (*sublink).subselect = select as *mut Node;
    (*sublink).location = (*ctor).location;

    transformExprRecurse(pstate, sublink as *mut Node)
}

/*
 * Common code for JSON_OBJECTAGG and JSON_ARRAYAGG transformation.
 */
unsafe fn transformJsonAggConstructor(
    pstate: *mut ParseState,
    agg_ctor: *mut crate::nodes::parsenodes::JsonAggConstructor,
    returning: *mut JsonReturning,
    args: *mut List,
    aggfnoid: Oid,
    aggtype: Oid,
    ctor_type: JsonConstructorType,
    unique: bool,
    absent_on_null: bool,
) -> *mut Node {
    let node: *mut Node;
    let aggfilter: *mut Expr;

    aggfilter = if !(*agg_ctor).agg_filter.is_null() {
        transformWhereClause(
            pstate,
            (*agg_ctor).agg_filter,
            EXPR_KIND_FILTER,
            cstr!("FILTER"),
        ) as *mut Expr
    } else {
        std::ptr::null_mut()
    };

    if !(*agg_ctor).over.is_null() {
        /* window function */
        let wfunc = makeNode!(WindowFunc, T_WindowFunc) as *mut WindowFunc;

        (*wfunc).winfnoid = aggfnoid;
        (*wfunc).wintype = aggtype;
        /* wincollid and inputcollid will be set by parse_collate.c */
        (*wfunc).args = args;
        (*wfunc).aggfilter = aggfilter;
        (*wfunc).runCondition = NIL;
        /* winref will be set by transformWindowFuncCall */
        (*wfunc).winstar = false;
        (*wfunc).winagg = true;
        (*wfunc).location = (*agg_ctor).location;

        /*
         * ordered aggs not allowed in windows yet
         */
        if !(*agg_ctor).agg_order.is_null() {
            ereport!(ERROR, errmsg!("aggregate ORDER BY is not implemented for window functions")) /* C also: errcode, parser_errposition */;
        }

        /* parse_agg.c does additional window-func-specific processing */
        transformWindowFuncCall(pstate, wfunc, (*agg_ctor).over as *mut c_void);

        node = wfunc as *mut Node;
    } else {
        let aggref = makeNode!(Aggref, T_Aggref) as *mut Aggref;

        (*aggref).aggfnoid = aggfnoid;
        (*aggref).aggtype = aggtype;

        /* aggcollid and inputcollid will be set by parse_collate.c */
        /* aggtranstype will be set by planner */
        /* aggargtypes will be set by transformAggregateCall */
        /* aggdirectargs and args will be set by transformAggregateCall */
        /* aggorder and aggdistinct will be set by transformAggregateCall */
        (*aggref).aggfilter = aggfilter;
        (*aggref).aggstar = false;
        (*aggref).aggvariadic = false;
        (*aggref).aggkind = AGGKIND_NORMAL;
        (*aggref).aggpresorted = false;
        /* agglevelsup will be set by transformAggregateCall */
        (*aggref).aggsplit = AggSplit::AGGSPLIT_SIMPLE; /* planner might change this */
        (*aggref).aggno = -1; /* planner will set aggno and aggtransno */
        (*aggref).aggtransno = -1;
        (*aggref).location = (*agg_ctor).location;

        transformAggregateCall(pstate, aggref, args, (*agg_ctor).agg_order, false);

        node = aggref as *mut Node;
    }

    makeJsonConstructorExpr(
        pstate,
        ctor_type,
        NIL,
        node as *mut Expr,
        returning,
        unique,
        absent_on_null,
        (*agg_ctor).location,
    )
}

/*
 * Transform JSON_OBJECTAGG() aggregate function.
 *
 * JSON_OBJECTAGG() is transformed into a JsonConstructorExpr node of type
 * JSCTOR_JSON_OBJECTAGG, which at runtime becomes a
 * json[b]_object_agg[_unique][_strict](agg->arg->key, agg->arg->value) call
 * depending on the output JSON format.  The result is coerced to the target
 * type given by agg->constructor->output.
 */
unsafe fn transformJsonObjectAgg(
    pstate: *mut ParseState,
    agg: *mut JsonObjectAgg,
) -> *mut Node {
    let returning: *mut JsonReturning;
    let key: *mut Node;
    let val: *mut Node;
    let args: *mut List;
    let aggfnoid: Oid;
    let aggtype: Oid;

    key = transformExprRecurse(pstate, (*(*agg).arg).key as *mut Node);
    val = transformJsonValueExpr(
        pstate,
        cstr!("JSON_OBJECTAGG()"),
        (*(*agg).arg).value,
        JS_FORMAT_DEFAULT,
        InvalidOid!(),
        false,
    );
    args = list_make2!(key, val);

    returning = transformJsonConstructorOutput(
        pstate,
        (*(*agg).constructor).output,
        args,
    );

    if (*(*returning).format).format_type == JS_FORMAT_JSONB {
        aggfnoid = if (*agg).absent_on_null {
            if (*agg).unique {
                F_JSONB_OBJECT_AGG_UNIQUE_STRICT
            } else {
                F_JSONB_OBJECT_AGG_STRICT
            }
        } else if (*agg).unique {
            F_JSONB_OBJECT_AGG_UNIQUE
        } else {
            F_JSONB_OBJECT_AGG
        };
        aggtype = JSONBOID;
    } else {
        aggfnoid = if (*agg).absent_on_null {
            if (*agg).unique {
                F_JSON_OBJECT_AGG_UNIQUE_STRICT
            } else {
                F_JSON_OBJECT_AGG_STRICT
            }
        } else if (*agg).unique {
            F_JSON_OBJECT_AGG_UNIQUE
        } else {
            F_JSON_OBJECT_AGG
        };
        aggtype = JSONOID;
    }

    transformJsonAggConstructor(
        pstate,
        (*agg).constructor,
        returning,
        args,
        aggfnoid,
        aggtype,
        JSCTOR_JSON_OBJECTAGG,
        (*agg).unique,
        (*agg).absent_on_null,
    )
}

/*
 * Transform JSON_ARRAYAGG() aggregate function.
 *
 * JSON_ARRAYAGG() is transformed into a JsonConstructorExpr node of type
 * JSCTOR_JSON_ARRAYAGG, which at runtime becomes a
 * json[b]_object_agg[_unique][_strict](agg->arg) call depending on the output
 * JSON format.  The result is coerced to the target type given by
 * agg->constructor->output.
 */
unsafe fn transformJsonArrayAgg(
    pstate: *mut ParseState,
    agg: *mut JsonArrayAgg,
) -> *mut Node {
    let returning: *mut JsonReturning;
    let arg: *mut Node;
    let aggfnoid: Oid;
    let aggtype: Oid;

    arg = transformJsonValueExpr(
        pstate,
        cstr!("JSON_ARRAYAGG()"),
        (*agg).arg,
        JS_FORMAT_DEFAULT,
        InvalidOid!(),
        false,
    );

    returning = transformJsonConstructorOutput(
        pstate,
        (*(*agg).constructor).output,
        list_make1!(arg),
    );

    if (*(*returning).format).format_type == JS_FORMAT_JSONB {
        aggfnoid = if (*agg).absent_on_null { F_JSONB_AGG_STRICT } else { F_JSONB_AGG };
        aggtype = JSONBOID;
    } else {
        aggfnoid = if (*agg).absent_on_null { F_JSON_AGG_STRICT } else { F_JSON_AGG };
        aggtype = JSONOID;
    }

    transformJsonAggConstructor(
        pstate,
        (*agg).constructor,
        returning,
        list_make1!(arg),
        aggfnoid,
        aggtype,
        JSCTOR_JSON_ARRAYAGG,
        false,
        (*agg).absent_on_null,
    )
}

/*
 * Transform JSON_ARRAY() constructor.
 *
 * JSON_ARRAY() is transformed into a JsonConstructorExpr node of type
 * JSCTOR_JSON_ARRAY.  The result is coerced to the target type given
 * by ctor->output.
 */
unsafe fn transformJsonArrayConstructor(
    pstate: *mut ParseState,
    ctor: *mut JsonArrayConstructor,
) -> *mut Node {
    let returning: *mut JsonReturning;
    let mut args: *mut List = NIL;

    /* transform element expressions, if any */
    if !(*ctor).exprs.is_null() {

        /* transform and append element arguments */
        foreach!(lc, (*ctor).exprs, {
            let jsval = castNode!(JsonValueExpr, T_JsonValueExpr, lfirst(current_cell!(lc))) as *mut ParseJsonValueExpr;
            let val = transformJsonValueExpr(
                pstate,
                cstr!("JSON_ARRAY()"),
                jsval,
                JS_FORMAT_DEFAULT,
                InvalidOid!(),
                false,
            );

            args = lappend(args, val as *mut c_void);
        });
    }

    returning = transformJsonConstructorOutput(pstate, (*ctor).output, args);

    makeJsonConstructorExpr(
        pstate,
        JSCTOR_JSON_ARRAY,
        args,
        std::ptr::null_mut(),
        returning,
        false,
        (*ctor).absent_on_null,
        (*ctor).location,
    )
}

unsafe fn transformJsonParseArg(
    pstate: *mut ParseState,
    jsexpr: *mut Node,
    format: *mut JsonFormat,
    exprtype_out: *mut Oid,
) -> *mut Node {
    let raw_expr = transformExprRecurse(pstate, jsexpr);
    let mut expr = raw_expr;

    *exprtype_out = exprType(expr);

    /* prepare input document */
    if *exprtype_out == BYTEAOID {
        let jve: *mut ParseJsonValueExpr;

        expr = raw_expr;
        expr = makeJsonByteaToTextConversion(expr, format, exprLocation(expr));
        *exprtype_out = TEXTOID;

        jve = makeJsonValueExpr(raw_expr as *mut Expr, expr as *mut Expr, format);
        expr = jve as *mut Node;
    } else {
        let mut typcategory: c_char = 0;
        let mut typispreferred: bool = false;

        get_type_category_preferred(*exprtype_out, &mut typcategory, &mut typispreferred);

        if *exprtype_out == UNKNOWNOID || typcategory == TYPCATEGORY_STRING {
            expr = coerce_to_target_type(
                pstate,
                expr,
                *exprtype_out,
                TEXTOID,
                -1,
                COERCION_IMPLICIT,
                COERCE_IMPLICIT_CAST,
                -1,
            );
            *exprtype_out = TEXTOID;
        }

        if (*format).encoding != JS_ENC_DEFAULT {
            ereport!(
                ERROR,
                errmsg!("invalid JSON encoding specification")
                /* C also: errcode, errdetail, errhint, parser_errposition */
            );
        }
    }

    expr
}

/*
 * Transform IS JSON predicate.
 */
unsafe fn transformJsonIsPredicate(
    pstate: *mut ParseState,
    pred: *mut JsonIsPredicate,
) -> *mut Node {
    let mut exprtype: Oid = 0;
    let expr = transformJsonParseArg(pstate, (*pred).expr, (*pred).format, &mut exprtype);

    /* make resulting expression */
    if exprtype != TEXTOID && exprtype != JSONOID && exprtype != JSONBOID {
        ereport!(
            ERROR,
            errmsg!("cannot use type {} in IS JSON predicate",
                     cstr_to_str(format_type_be(exprtype)))
            /* C also: errcode, errdetail, errhint, parser_errposition */
        );
    }

    /* This intentionally(?) drops the format clause. */
    makeJsonIsPredicate(
        expr,
        std::ptr::null_mut(),
        (*pred).item_type,
        (*pred).unique_keys,
        (*pred).location,
    ) as *mut Node
}

/*
 * Transform the RETURNING clause of a JSON_*() expression if there is one and
 * create one if not.
 */
unsafe fn transformJsonReturning(
    pstate: *mut ParseState,
    output: *mut JsonOutput,
    fname: *const c_char,
) -> *mut JsonReturning {
    let returning: *mut JsonReturning;

    if !output.is_null() {
        returning = transformJsonOutput(pstate, output, false);

        Assert!(OidIsValid((*returning).typid));

        if (*returning).typid != JSONOID && (*returning).typid != JSONBOID {
            ereport!(
                ERROR,
                errmsg!("cannot use type {} in RETURNING clause of {}",
                         cstr_to_str(format_type_be((*returning).typid)), cstr_to_str(fname))
                /* C also: errcode, errdetail, errhint, parser_errposition */
            );
        }
    } else {
        /* Output type is JSON by default. */
        let targettype: Oid = JSONOID;
        let format = JS_FORMAT_JSON;

        returning = makeNode!(JsonReturning, T_JsonReturning) as *mut JsonReturning;
        (*returning).format = makeJsonFormat(format, JS_ENC_DEFAULT, -1);
        (*returning).typid = targettype;
        (*returning).typmod = -1;
    }

    returning
}

/*
 * Transform a JSON() expression.
 *
 * JSON() is transformed into a JsonConstructorExpr of type JSCTOR_JSON_PARSE,
 * which validates the input expression value as JSON.
 */
unsafe fn transformJsonParseExpr(
    pstate: *mut ParseState,
    jsexpr: *mut JsonParseExpr,
) -> *mut Node {
    let output = (*jsexpr).output;
    let returning: *mut JsonReturning;
    let arg: *mut Node;

    returning = transformJsonReturning(pstate, output, cstr!("JSON()"));

    if (*jsexpr).unique_keys {
        /*
         * Coerce string argument to text and then to json[b] in the executor
         * node with key uniqueness check.
         */
        let jve = (*jsexpr).expr;
        let mut arg_type: Oid = 0;

        arg = transformJsonParseArg(
            pstate,
            (*jve).raw_expr as *mut Node,
            (*jve).format,
            &mut arg_type,
        );

        if arg_type != TEXTOID {
            ereport!(
                ERROR,
                errmsg!("cannot use non-string types with WITH UNIQUE KEYS clause")
                /* C also: errcode, errdetail, errhint, parser_errposition */
            );
        }
    } else {
        /*
         * Coerce argument to target type using CAST for compatibility with PG
         * function-like CASTs.
         */
        arg = transformJsonValueExpr(
            pstate,
            cstr!("JSON()"),
            (*jsexpr).expr,
            JS_FORMAT_JSON,
            (*returning).typid,
            false,
        );
    }

    makeJsonConstructorExpr(
        pstate,
        JSCTOR_JSON_PARSE,
        list_make1!(arg),
        std::ptr::null_mut(),
        returning,
        (*jsexpr).unique_keys,
        false,
        (*jsexpr).location,
    )
}

/*
 * Transform a JSON_SCALAR() expression.
 *
 * JSON_SCALAR() is transformed into a JsonConstructorExpr of type
 * JSCTOR_JSON_SCALAR, which converts the input SQL scalar value into
 * a json[b] value.
 */
unsafe fn transformJsonScalarExpr(
    pstate: *mut ParseState,
    jsexpr: *mut JsonScalarExpr,
) -> *mut Node {
    let mut arg = transformExprRecurse(pstate, (*jsexpr).expr as *mut Node);
    let output = (*jsexpr).output;
    let returning: *mut JsonReturning;

    returning = transformJsonReturning(pstate, output, cstr!("JSON_SCALAR()"));

    if exprType(arg) == UNKNOWNOID {
        arg = coerce_to_specific_type(pstate, arg, TEXTOID, cstr!("JSON_SCALAR"));
    }

    makeJsonConstructorExpr(
        pstate,
        JSCTOR_JSON_SCALAR,
        list_make1!(arg),
        std::ptr::null_mut(),
        returning,
        false,
        false,
        (*jsexpr).location,
    )
}

/*
 * Transform a JSON_SERIALIZE() expression.
 *
 * JSON_SERIALIZE() is transformed into a JsonConstructorExpr of type
 * JSCTOR_JSON_SERIALIZE which converts the input JSON value into a character
 * or bytea string.
 */
unsafe fn transformJsonSerializeExpr(
    pstate: *mut ParseState,
    expr: *mut JsonSerializeExpr,
) -> *mut Node {
    let returning: *mut JsonReturning;
    let arg = transformJsonValueExpr(
        pstate,
        cstr!("JSON_SERIALIZE()"),
        (*expr).expr,
        JS_FORMAT_JSON,
        InvalidOid!(),
        false,
    );

    if !(*expr).output.is_null() {
        returning = transformJsonOutput(pstate, (*expr).output, true);

        if (*returning).typid != BYTEAOID {
            let mut typcategory: c_char = 0;
            let mut typispreferred: bool = false;

            get_type_category_preferred(
                (*returning).typid,
                &mut typcategory,
                &mut typispreferred,
            );
            if typcategory != TYPCATEGORY_STRING {
                ereport!(
                    ERROR,
                    errmsg!("cannot use type {} in RETURNING clause of {}",
                             cstr_to_str(format_type_be((*returning).typid)),
                             "JSON_SERIALIZE()")
                    /* C also: errcode, errdetail, errhint, parser_errposition */
                );
            }
        }
    } else {
        /* RETURNING TEXT FORMAT JSON is by default */
        returning = makeNode!(JsonReturning, T_JsonReturning) as *mut JsonReturning;
        (*returning).format = makeJsonFormat(JS_FORMAT_JSON, JS_ENC_DEFAULT, -1);
        (*returning).typid = TEXTOID;
        (*returning).typmod = -1;
    }

    makeJsonConstructorExpr(
        pstate,
        JSCTOR_JSON_SERIALIZE,
        list_make1!(arg),
        std::ptr::null_mut(),
        returning,
        false,
        false,
        (*expr).location,
    )
}

/*
 * Transform JSON_VALUE, JSON_QUERY, JSON_EXISTS, JSON_TABLE functions into
 * a JsonExpr node.
 */
unsafe fn transformJsonFuncExpr(
    pstate: *mut ParseState,
    func: *mut JsonFuncExpr,
) -> *mut Node {
    let jsexpr: *mut JsonExpr;
    let path_spec: *mut Node;
    let pathspec_type: Oid;
    let pathspec_loc: c_int;
    let coerced_path_spec: *mut Node;
    let func_name: *const c_char;
    let default_format: JsonFormatType;

    #[allow(unreachable_patterns)]
    match (*func).op {
        JSON_EXISTS_OP => {
            func_name = cstr!("JSON_EXISTS");
            default_format = JS_FORMAT_DEFAULT;
        }
        JSON_QUERY_OP => {
            func_name = cstr!("JSON_QUERY");
            default_format = JS_FORMAT_JSONB;
        }
        JSON_VALUE_OP => {
            func_name = cstr!("JSON_VALUE");
            default_format = JS_FORMAT_DEFAULT;
        }
        JSON_TABLE_OP => {
            func_name = cstr!("JSON_TABLE");
            default_format = JS_FORMAT_JSONB;
        }
        _ => {
            elog!(ERROR, "invalid JsonFuncExpr op {}", (*func).op as c_int);
            func_name = std::ptr::null();
            default_format = JS_FORMAT_DEFAULT; /* keep compiler quiet */
        }
    }

    /*
     * Even though the syntax allows it, FORMAT JSON specification in
     * RETURNING is meaningless except for JSON_QUERY().  Flag if not
     * JSON_QUERY().
     */
    if !(*func).output.is_null() && (*func).op != JSON_QUERY_OP {
        let format = (*(*(*func).output).returning).format;

        if (*format).format_type != JS_FORMAT_DEFAULT
            || (*format).encoding != JS_ENC_DEFAULT
        {
            ereport!(ERROR, errmsg!("cannot specify FORMAT JSON in RETURNING clause of {}()",
                        cstr_to_str(func_name))) /* C also: errcode, parser_errposition */;
        }
    }

    /* OMIT QUOTES is meaningless when strings are wrapped. */
    if (*func).op == JSON_QUERY_OP {
        if (*func).quotes == JS_QUOTES_OMIT
            && ((*func).wrapper == JSW_CONDITIONAL || (*func).wrapper == JSW_UNCONDITIONAL)
        {
            ereport!(ERROR, errmsg!("SQL/JSON QUOTES behavior must not be specified when WITH WRAPPER is used")) /* C also: errcode, parser_errposition */;
        }
        if !(*func).on_empty.is_null() {
            let bt = (*(*func).on_empty).btype;
            if bt != JSON_BEHAVIOR_ERROR
                && bt != JSON_BEHAVIOR_NULL
                && bt != JSON_BEHAVIOR_EMPTY
                && bt != JSON_BEHAVIOR_EMPTY_ARRAY
                && bt != JSON_BEHAVIOR_EMPTY_OBJECT
                && bt != JSON_BEHAVIOR_DEFAULT
            {
                if (*func).column_name.is_null() {
                    ereport!(
                        ERROR,
                        errmsg!("invalid {} behavior", "ON EMPTY")
                        /* C also: errcode, parser_errposition */
                    );
                } else {
                    ereport!(
                        ERROR,
                        errmsg!("invalid {} behavior for column \"{}\"",
                                "ON EMPTY", cstr_to_str((*func).column_name))
                        /* C also: errcode, parser_errposition */
                    );
                }
            }
        }
        if !(*func).on_error.is_null() {
            let bt = (*(*func).on_error).btype;
            if bt != JSON_BEHAVIOR_ERROR
                && bt != JSON_BEHAVIOR_NULL
                && bt != JSON_BEHAVIOR_EMPTY
                && bt != JSON_BEHAVIOR_EMPTY_ARRAY
                && bt != JSON_BEHAVIOR_EMPTY_OBJECT
                && bt != JSON_BEHAVIOR_DEFAULT
            {
                if (*func).column_name.is_null() {
                    ereport!(
                        ERROR,
                        errmsg!("invalid {} behavior", "ON ERROR")
                        /* C also: errcode, parser_errposition */
                    );
                } else {
                    ereport!(
                        ERROR,
                        errmsg!("invalid {} behavior for column \"{}\"",
                                "ON ERROR", cstr_to_str((*func).column_name))
                        /* C also: errcode, parser_errposition */
                    );
                }
            }
        }
    }

    /* Check that ON ERROR/EMPTY behavior values are valid for the function. */
    if (*func).op == JSON_EXISTS_OP
        && !(*func).on_error.is_null()
    {
        let bt = (*(*func).on_error).btype;
        if bt != JSON_BEHAVIOR_ERROR
            && bt != JSON_BEHAVIOR_TRUE
            && bt != JSON_BEHAVIOR_FALSE
            && bt != JSON_BEHAVIOR_UNKNOWN
        {
            if (*func).column_name.is_null() {
                ereport!(
                    ERROR,
                    errmsg!("invalid {} behavior", "ON ERROR")
                    /* C also: errcode, parser_errposition */
                );
            } else {
                ereport!(
                    ERROR,
                    errmsg!("invalid {} behavior for column \"{}\"",
                            "ON ERROR", cstr_to_str((*func).column_name))
                    /* C also: errcode, parser_errposition */
                );
            }
        }
    }

    if (*func).op == JSON_VALUE_OP {
        if !(*func).on_empty.is_null() {
            let bt = (*(*func).on_empty).btype;
            if bt != JSON_BEHAVIOR_ERROR
                && bt != JSON_BEHAVIOR_NULL
                && bt != JSON_BEHAVIOR_DEFAULT
            {
                if (*func).column_name.is_null() {
                    ereport!(
                        ERROR,
                        errmsg!("invalid {} behavior", "ON EMPTY")
                        /* C also: errcode, parser_errposition */
                    );
                } else {
                    ereport!(
                        ERROR,
                        errmsg!("invalid {} behavior for column \"{}\"",
                                "ON EMPTY", cstr_to_str((*func).column_name))
                        /* C also: errcode, parser_errposition */
                    );
                }
            }
        }
        if !(*func).on_error.is_null() {
            let bt = (*(*func).on_error).btype;
            if bt != JSON_BEHAVIOR_ERROR
                && bt != JSON_BEHAVIOR_NULL
                && bt != JSON_BEHAVIOR_DEFAULT
            {
                if (*func).column_name.is_null() {
                    ereport!(
                        ERROR,
                        errmsg!("invalid {} behavior", "ON ERROR")
                        /* C also: errcode, parser_errposition */
                    );
                } else {
                    ereport!(
                        ERROR,
                        errmsg!("invalid {} behavior for column \"{}\"",
                                "ON ERROR", cstr_to_str((*func).column_name))
                        /* C also: errcode, parser_errposition */
                    );
                }
            }
        }
    }

    jsexpr = makeNode!(JsonExpr, T_JsonExpr) as *mut JsonExpr;
    (*jsexpr).location = (*func).location;
    (*jsexpr).op = (*func).op;
    (*jsexpr).column_name = (*func).column_name;

    /*
     * jsonpath machinery can only handle jsonb documents, so coerce the input
     * if not already of jsonb type.
     */
    (*jsexpr).formatted_expr = transformJsonValueExpr(
        pstate,
        func_name,
        (*func).context_item,
        default_format,
        JSONBOID,
        false,
    );
    (*jsexpr).format = (*(*func).context_item).format;

    let ps = transformExprRecurse(pstate, (*func).pathspec);
    let pt = exprType(ps);
    let pl = exprLocation(ps);
    let cps = coerce_to_target_type(
        pstate, ps, pt, JSONPATHOID, -1,
        COERCION_EXPLICIT, COERCE_IMPLICIT_CAST, pl,
    );
    if cps.is_null() {
        ereport!(
            ERROR,
            errmsg!("JSON path expression must be of type {}, not of type {}",
                    "jsonpath", cstr_to_str(format_type_be(pt)))
            /* C also: errcode, parser_errposition */
        );
    }
    (*jsexpr).path_spec = cps;

    /* Transform and coerce the PASSING arguments to jsonb. */
    transformJsonPassingArgs(
        pstate,
        func_name,
        JS_FORMAT_JSONB,
        (*func).passing,
        &mut (*jsexpr).passing_values,
        &mut (*jsexpr).passing_names,
    );

    /* Transform the JsonOutput into JsonReturning. */
    (*jsexpr).returning = transformJsonOutput(pstate, (*func).output, false);

    #[allow(unreachable_patterns)]
    match (*func).op {
        JSON_EXISTS_OP => {
            /* JSON_EXISTS returns boolean by default. */
            if !OidIsValid((*(*jsexpr).returning).typid) {
                (*(*jsexpr).returning).typid = BOOLOID;
                (*(*jsexpr).returning).typmod = -1;
                (*jsexpr).collation = InvalidOid!();
            }

            /* JSON_TABLE() COLUMNS can specify a non-boolean type. */
            if (*(*jsexpr).returning).typid != BOOLOID {
                (*jsexpr).use_json_coercion = true;
            }

            (*jsexpr).on_error = transformJsonBehavior(
                pstate,
                jsexpr,
                (*func).on_error,
                JSON_BEHAVIOR_FALSE,
                (*jsexpr).returning,
            );
        }
        JSON_QUERY_OP => {
            /* JSON_QUERY returns jsonb by default. */
            if !OidIsValid((*(*jsexpr).returning).typid) {
                let ret = (*jsexpr).returning;
                (*ret).typid = JSONBOID;
                (*ret).typmod = -1;
            }

            (*jsexpr).collation = get_typcollation((*(*jsexpr).returning).typid);

            /*
             * Keep quotes on scalar strings by default, omitting them only if
             * OMIT QUOTES is specified.
             */
            (*jsexpr).omit_quotes = (*func).quotes == JS_QUOTES_OMIT;
            (*jsexpr).wrapper = (*func).wrapper;

            /*
             * Set up to coerce the result value of JsonPathValue() to the
             * RETURNING type (default or user-specified), if needed.  Also if
             * OMIT QUOTES is specified.
             */
            if (*(*jsexpr).returning).typid != JSONBOID || (*jsexpr).omit_quotes {
                (*jsexpr).use_json_coercion = true;
            }

            /* Assume NULL ON EMPTY when ON EMPTY is not specified. */
            (*jsexpr).on_empty = transformJsonBehavior(
                pstate,
                jsexpr,
                (*func).on_empty,
                JSON_BEHAVIOR_NULL,
                (*jsexpr).returning,
            );
            /* Assume NULL ON ERROR when ON ERROR is not specified. */
            (*jsexpr).on_error = transformJsonBehavior(
                pstate,
                jsexpr,
                (*func).on_error,
                JSON_BEHAVIOR_NULL,
                (*jsexpr).returning,
            );
        }
        JSON_VALUE_OP => {
            /* JSON_VALUE returns text by default. */
            if !OidIsValid((*(*jsexpr).returning).typid) {
                (*(*jsexpr).returning).typid = TEXTOID;
                (*(*jsexpr).returning).typmod = -1;
            }
            (*jsexpr).collation = get_typcollation((*(*jsexpr).returning).typid);

            /*
             * Override whatever transformJsonOutput() set these to, which
             * assumes that output type to be jsonb.
             */
            (*(*(*jsexpr).returning).format).format_type = JS_FORMAT_DEFAULT;
            (*(*(*jsexpr).returning).format).encoding = JS_ENC_DEFAULT;

            /* Always omit quotes from scalar strings. */
            (*jsexpr).omit_quotes = true;

            /*
             * Set up to coerce the result value of JsonPathValue() to the
             * RETURNING type (default or user-specified), if needed.
             */
            if (*(*jsexpr).returning).typid != TEXTOID {
                if get_typtype((*(*jsexpr).returning).typid) == TYPTYPE_DOMAIN
                    && DomainHasConstraints((*(*jsexpr).returning).typid)
                {
                    (*jsexpr).use_json_coercion = true;
                } else {
                    (*jsexpr).use_io_coercion = true;
                }
            }

            /* Assume NULL ON EMPTY when ON EMPTY is not specified. */
            (*jsexpr).on_empty = transformJsonBehavior(
                pstate,
                jsexpr,
                (*func).on_empty,
                JSON_BEHAVIOR_NULL,
                (*jsexpr).returning,
            );
            /* Assume NULL ON ERROR when ON ERROR is not specified. */
            (*jsexpr).on_error = transformJsonBehavior(
                pstate,
                jsexpr,
                (*func).on_error,
                JSON_BEHAVIOR_NULL,
                (*jsexpr).returning,
            );
        }
        JSON_TABLE_OP => {
            if !OidIsValid((*(*jsexpr).returning).typid) {
                (*(*jsexpr).returning).typid = exprType((*jsexpr).formatted_expr);
                (*(*jsexpr).returning).typmod = -1;
            }
            (*jsexpr).collation = get_typcollation((*(*jsexpr).returning).typid);

            /*
             * Assume EMPTY ARRAY ON ERROR when ON ERROR is not specified.
             *
             * ON EMPTY cannot be specified at the top level but it can be for
             * the individual columns.
             */
            (*jsexpr).on_error = transformJsonBehavior(
                pstate,
                jsexpr,
                (*func).on_error,
                JSON_BEHAVIOR_EMPTY_ARRAY,
                (*jsexpr).returning,
            );
        }
        _ => {
            elog!(ERROR, "invalid JsonFuncExpr op {}", (*func).op as c_int);
        }
    }

    jsexpr as *mut Node
}

/*
 * Transform a SQL/JSON PASSING clause.
 */
unsafe fn transformJsonPassingArgs(
    pstate: *mut ParseState,
    construct_name: *const c_char,
    format: JsonFormatType,
    args: *mut List,
    passing_values: *mut *mut List,
    passing_names: *mut *mut List,
) {

    *passing_values = NIL;
    *passing_names = NIL;

    foreach!(lc, args, {
        let arg = castNode!(JsonArgument, T_JsonArgument, lfirst(current_cell!(lc))) as *mut JsonArgument;
        let expr = transformJsonValueExpr(
            pstate,
            construct_name,
            (*arg).val,
            format,
            InvalidOid!(),
            true,
        );

        *passing_values = lappend(*passing_values, expr as *mut c_void);
        *passing_names = lappend(*passing_names, makeString((*arg).name) as *mut c_void);
    });
}

/*
 * Recursively checks if the given expression, or its sub-node in some cases,
 * is valid for using as an ON ERROR / ON EMPTY DEFAULT expression.
 */
unsafe fn ValidJsonBehaviorDefaultExpr(expr: *mut Node, context: *mut c_void) -> bool {
    if expr.is_null() {
        return false;
    }

    match nodeTag(expr) {
        /* Acceptable expression nodes */
        T_Const | T_FuncExpr | T_OpExpr => {
            return true;
        }

        /* Acceptable iff arg of the following nodes is one of the above */
        T_CoerceViaIO
        | T_CoerceToDomain
        | T_ArrayCoerceExpr
        | T_ConvertRowtypeExpr
        | T_RelabelType
        | T_CollateExpr => {
            return expression_tree_walker(
                expr,
                Some(std::mem::transmute::<
                    unsafe fn(*mut Node, *mut c_void) -> bool,
                    unsafe fn(*mut Node, *mut c_void) -> bool,
                >(ValidJsonBehaviorDefaultExpr)),
                context,
            );
        }
        _ => {}
    }

    false
}

/*
 * Transform a JSON BEHAVIOR clause.
 */
unsafe fn transformJsonBehavior(
    pstate: *mut ParseState,
    jsexpr: *mut JsonExpr,
    behavior: *mut crate::nodes::primnodes::JsonBehavior,
    default_behavior: JsonBehaviorType,
    returning: *mut JsonReturning,
) -> *mut crate::nodes::primnodes::JsonBehavior {
    let mut btype = default_behavior;
    let mut expr: *mut Node = std::ptr::null_mut();
    let mut coerce_at_runtime = false;
    let mut location: c_int = -1;

    if !behavior.is_null() {
        btype = (*behavior).btype;
        location = (*behavior).location;
        if btype == JSON_BEHAVIOR_DEFAULT {
            let targetcoll = (*jsexpr).collation;
            let exprcoll: Oid;

            expr = transformExprRecurse(pstate, (*behavior).expr);

            if !ValidJsonBehaviorDefaultExpr(expr, std::ptr::null_mut()) {
                ereport!(
                    ERROR,
                    errmsg!("can only specify a constant, non-aggregate function, or operator expression for DEFAULT")
                    /* C also: errcode, errdetail, errhint, parser_errposition */
                );
            }
            if contain_var_clause(expr) {
                ereport!(
                    ERROR,
                    errmsg!("DEFAULT expression must not contain column references")
                    /* C also: errcode, errdetail, errhint, parser_errposition */
                );
            }
            if expression_returns_set(expr) {
                ereport!(
                    ERROR,
                    errmsg!("DEFAULT expression must not return a set")
                    /* C also: errcode, errdetail, errhint, parser_errposition */
                );
            }

            /*
             * Reject a DEFAULT expression whose collation differs from the
             * enclosing JSON expression's result collation
             * (jsexpr->collation), as chosen by the RETURNING clause.
             */
            exprcoll = {
                let ec = exprCollation(expr);
                if !OidIsValid(ec) { get_typcollation(exprType(expr)) } else { ec }
            };
            if OidIsValid(targetcoll) && OidIsValid(exprcoll) && targetcoll != exprcoll {
                ereport!(ERROR, errmsg!("collation of DEFAULT expression conflicts with RETURNING clause")) /* C also: errcode, errdetail, parser_errposition */;
            }
        }
    }

    if expr.is_null() && btype != JSON_BEHAVIOR_ERROR {
        expr = GetJsonBehaviorConst(btype, location);
    }

    /*
     * Try to coerce the expression if needed.
     *
     * Use runtime coercion using json_populate_type() if the expression is
     * NULL, jsonb-valued, or boolean-valued (unless the target type is
     * integer or domain over integer, in which case use the
     * boolean-to-integer cast function).
     *
     * For other non-NULL expressions, try to find a cast and error out if one
     * is not found.
     */
    if !expr.is_null() && exprType(expr) != (*returning).typid {
        let isnull = IsA!(expr, T_Const) && (*(expr as *mut crate::nodes::primnodes::Const)).constisnull;

        if isnull
            || exprType(expr) == JSONBOID
            || (exprType(expr) == BOOLOID
                && getBaseType((*returning).typid) != INT4OID)
        {
            coerce_at_runtime = true;

            /*
             * json_populate_type() expects to be passed a jsonb value, so gin
             * up a Const containing the appropriate boolean value represented
             * as jsonb, discarding the original Const containing a plain
             * boolean.
             */
            if exprType(expr) == BOOLOID {
                let val: *const c_char = if btype == JSON_BEHAVIOR_TRUE {
                    cstr!("true")
                } else {
                    cstr!("false")
                };

                expr = makeConst(
                    JSONBOID,
                    -1,
                    InvalidOid!(),
                    -1,
                    crate::utils::fmgr::DirectFunctionCall1Coll(
                        jsonb_stub::jsonb_in,
                        InvalidOid!(),
                        CStringGetDatum(val),
                    ),
                    false,
                    false,
                ) as *mut Node;
            }
        } else {
            let coerced_expr: *mut Node;
            let typcategory = TypeCategory((*returning).typid);

            /*
             * Use an assignment cast if coercing to a string type so that
             * build_coercion_expression() assumes implicit coercion when
             * coercing the typmod, so that inputs exceeding length cause an
             * error instead of silent truncation.
             */
            let coercion_context = if typcategory == TYPCATEGORY_STRING
                || typcategory == TYPCATEGORY_BITSTRING
            {
                COERCION_ASSIGNMENT
            } else {
                COERCION_EXPLICIT
            };

            coerced_expr = coerce_to_target_type(
                pstate,
                expr,
                exprType(expr),
                (*returning).typid,
                (*returning).typmod,
                coercion_context,
                COERCE_EXPLICIT_CAST,
                exprLocation(behavior as *const Node),
            );

            if coerced_expr.is_null() {
                /*
                 * Provide a HINT if the expression comes from a DEFAULT
                 * clause.
                 */
                if btype == JSON_BEHAVIOR_DEFAULT {
                    ereport!(ERROR, errmsg!("cannot cast behavior expression of type {} to {}",
                                cstr_to_str(format_type_be(exprType(expr))),
                                cstr_to_str(format_type_be((*returning).typid)))) /* C also: errcode, errhint, parser_errposition */;
                } else {
                    ereport!(ERROR, errmsg!("cannot cast behavior expression of type {} to {}",
                                cstr_to_str(format_type_be(exprType(expr))),
                                cstr_to_str(format_type_be((*returning).typid)))) /* C also: errcode, parser_errposition */;
                }
            }

            expr = coerced_expr;
        }
    }

    let ret_behavior = if !behavior.is_null() {
        (*behavior).expr = expr;
        behavior
    } else {
        makeJsonBehavior(btype, expr, location)
    };

    (*ret_behavior).coerce = coerce_at_runtime;

    ret_behavior
}

/*
 * Returns a Const node holding the value for the given non-ERROR
 * JsonBehaviorType.
 */
unsafe fn GetJsonBehaviorConst(btype: JsonBehaviorType, location: c_int) -> *mut Node {
    let mut val: Datum = crate::postgres::Datum::from(0usize);
    let mut typid: Oid = JSONBOID;
    let mut len: i32 = -1;
    let mut isbyval = false;
    let mut isnull = false;
    let con: *mut crate::nodes::primnodes::Const;

    #[allow(unreachable_patterns)]
    match btype {
        JSON_BEHAVIOR_EMPTY_ARRAY => {
            val = crate::utils::fmgr::DirectFunctionCall1Coll(
                jsonb_stub::jsonb_in,
                InvalidOid!(),
                CStringGetDatum(cstr!("[]")),
            );
        }
        JSON_BEHAVIOR_EMPTY_OBJECT => {
            val = crate::utils::fmgr::DirectFunctionCall1Coll(
                jsonb_stub::jsonb_in,
                InvalidOid!(),
                CStringGetDatum(cstr!("{}")),
            );
        }
        JSON_BEHAVIOR_TRUE => {
            val = BoolGetDatum(true);
            typid = BOOLOID;
            len = std::mem::size_of::<bool>() as i32;
            isbyval = true;
        }
        JSON_BEHAVIOR_FALSE => {
            val = BoolGetDatum(false);
            typid = BOOLOID;
            len = std::mem::size_of::<bool>() as i32;
            isbyval = true;
        }
        JSON_BEHAVIOR_NULL | JSON_BEHAVIOR_UNKNOWN | JSON_BEHAVIOR_EMPTY => {
            val = crate::postgres::Datum::from(0usize);
            isnull = true;
            typid = INT4OID;
            len = std::mem::size_of::<i32>() as i32;
            isbyval = true;
        }
        /* These two behavior types are handled by the caller. */
        JSON_BEHAVIOR_DEFAULT | JSON_BEHAVIOR_ERROR => {
            Assert!(false);
        }
        _ => {
            elog!(ERROR, "unrecognized SQL/JSON behavior {}", btype as c_int);
        }
    }

    con = makeConst(typid, -1, InvalidOid!(), len, val, isnull, isbyval);
    (*con).location = location;

    con as *mut Node
}

/* Stub for type_is_array -- not in lsyscache.rs yet */
unsafe fn type_is_array(typid: Oid) -> bool {
    OidIsValid(get_element_type(typid))
}

/* Stub for TypeCategory -- not yet ported */
unsafe fn TypeCategory(typid: Oid) -> c_char {
    let mut typcategory: c_char = 0;
    let mut typispreferred: bool = false;
    get_type_category_preferred(typid, &mut typcategory, &mut typispreferred);
    typcategory
}

/* Stub for count_nonjunk_tlist_entries */
unsafe fn count_nonjunk_tlist_entries(tlist: *mut List) -> c_int {
    crate::optimizer::optimizer::count_nonjunk_tlist_entries(tlist)
}

/* Stub for contain_var_clause */
unsafe fn contain_var_clause(node: *mut Node) -> bool {
    crate::optimizer::optimizer::contain_var_clause(node)
}

/* Stub for contain_vars_of_level */
unsafe fn contain_vars_of_level(node: *mut Node, levelsup: c_int) -> bool {
    crate::optimizer::optimizer::contain_vars_of_level(node, levelsup)
}
