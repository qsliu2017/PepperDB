//! nodes/readfuncs.c - Reader functions for Postgres tree nodes.
//!
//! NOTES
//!   Parse location fields are written out by outfuncs.c, but only for
//!   debugging use.  When reading a location field, we normally discard
//!   the stored value and set the location field to -1 (ie, "unknown").
//!   This is because nodes coming from a stored rule should not be thought
//!   to have a known location in the current query's text.
//!
//!   However, if restore_location_fields is true, we do restore location
//!   fields from the string.  This is currently intended only for use by the
//!   debug_write_read_parse_plan_trees test code, which doesn't want to cause
//!   any change in the node contents.
//!
//! src/backend/nodes/readfuncs.c
//! src/include/nodes/readfuncs.h

use crate::prelude::*;

use crate::miscadmin::check_stack_depth;
use crate::nodes::bitmapset::{bms_add_member, Bitmapset};
use crate::nodes::extensible::{ExtensibleNode, ExtensibleNodeMethods, GetExtensibleNodeMethods};
use crate::nodes::nodes::{nodeTag, newNode, JoinType, Node, NodeTag, ParseLoc};
use crate::nodes::parsenodes::{
    A_Const, A_Expr, A_Expr_Kind::*, RTEKind, RTEKind::*, RangeTblEntry,
};
use crate::nodes::primnodes::{BoolExpr, BoolExprType::*, Const, TableFunc};
use crate::nodes::read::{debackslash, nodeRead, pg_strtok};
use crate::nodes::value::{BitString, Boolean, Float, Integer, String as PgString};
use crate::{elog, makeNode};

use std::ffi::{c_char, c_int};

// `restore_location_fields` lives in read.rs.
#[cfg(debug_assertions)]
use crate::nodes::read::restore_location_fields;

extern "C" {
    fn atoi(s: *const c_char) -> c_int;
    fn atol(s: *const c_char) -> std::ffi::c_long;
    fn atof(s: *const c_char) -> f64;
    fn strtoul(s: *const c_char, endptr: *mut *mut c_char, base: c_int) -> std::ffi::c_ulong;
    fn strtoll(s: *const c_char, endptr: *mut *mut c_char, base: c_int) -> i64;
    fn strtoull(s: *const c_char, endptr: *mut *mut c_char, base: c_int) -> u64;
    fn strtol(s: *const c_char, endptr: *mut *mut c_char, base: c_int) -> std::ffi::c_long;
    fn strncmp(s1: *const c_char, s2: *const c_char, n: usize) -> c_int;
    fn memcmp(s1: *const c_void, s2: *const c_void, n: usize) -> c_int;
    fn memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
}

/*
 * Macros to simplify reading of different kinds of fields.  Use these
 * wherever possible to reduce the chance for silly typos.  Note that these
 * hard-wire conventions about the names of the local variables in a Read
 * routine.
 *
 * In Rust the C textual-substitution macros are expressed as helper functions
 * plus inline code in each Read routine.  The C macros depend on the local
 * variables `token`, `length`, and `local_node`.
 */

/*
 * NOTE: use atoi() to read values written with %d, or atoui() to read
 * values written with %u in outfuncs.c.  An exception is OID values,
 * for which use atooid().  (As of 7.1, outfuncs.c writes OIDs as %u,
 * but this will probably change in the future.)
 */
#[inline]
unsafe fn atoui(x: *const c_char) -> u32 {
    strtoul(x, std::ptr::null_mut(), 10) as u32
}

#[inline]
unsafe fn atooid(x: *const c_char) -> Oid {
    strtoul(x, std::ptr::null_mut(), 10) as u32 as Oid
}

#[inline]
unsafe fn strtobool(x: *const c_char) -> bool {
    *x == b't' as c_char
}

#[inline]
unsafe fn strtoi64(s: *const c_char, endptr: *mut *mut c_char, base: c_int) -> i64 {
    strtoll(s, endptr, base)
}

#[inline]
unsafe fn strtou64(s: *const c_char, endptr: *mut *mut c_char, base: c_int) -> u64 {
    strtoull(s, endptr, base)
}

unsafe fn nullable_string(token: *const c_char, length: c_int) -> *mut c_char {
    /* outToken emits <> for NULL, and pg_strtok makes that an empty string */
    if length == 0 {
        return std::ptr::null_mut();
    }
    /* outToken emits "" for empty string */
    if length == 2 && *token == b'"' as c_char && *token.add(1) == b'"' as c_char {
        return pstrdup(c"".as_ptr());
    }
    /* otherwise, we must remove protective backslashes added by outToken */
    debackslash(token, length)
}

/*
 * _readBitmapset
 *
 * Note: this code is used in contexts where we know that a Bitmapset
 * is expected.  There is equivalent code in nodeRead() that can read a
 * Bitmapset when we come across one in other contexts.
 */
unsafe fn _readBitmapset() -> *mut Bitmapset {
    let mut result: *mut Bitmapset = std::ptr::null_mut();

    let mut length: c_int = 0;
    let mut token: *const c_char;

    token = pg_strtok(&mut length);
    if token.is_null() {
        elog!(ERROR, "incomplete Bitmapset structure");
    }
    if length != 1 || *token != b'(' as c_char {
        elog!(ERROR, "unrecognized token: \"{}\"", show_token(token, length));
    }

    token = pg_strtok(&mut length);
    if token.is_null() {
        elog!(ERROR, "incomplete Bitmapset structure");
    }
    if length != 1 || *token != b'b' as c_char {
        elog!(ERROR, "unrecognized token: \"{}\"", show_token(token, length));
    }

    loop {
        token = pg_strtok(&mut length);
        if token.is_null() {
            elog!(ERROR, "unterminated Bitmapset structure");
        }
        if length == 1 && *token == b')' as c_char {
            break;
        }
        let mut endptr: *mut c_char = std::ptr::null_mut();
        let val = strtol(token, &mut endptr, 10) as c_int;
        if endptr != token.add(length as usize) as *mut c_char {
            elog!(ERROR, "unrecognized integer: \"{}\"", show_token(token, length));
        }
        result = bms_add_member(result, val);
    }

    result
}

/*
 * We export this function for use by extensions that define extensible nodes.
 * That's somewhat historical, though, because calling nodeRead() will work.
 */
pub unsafe fn readBitmapset() -> *mut Bitmapset {
    _readBitmapset()
}

/*
 * Helper to render a (non-null-terminated) pg_strtok token of the given length
 * for an error message (replaces the C "%.*s" format).
 */
unsafe fn show_token(token: *const c_char, length: c_int) -> std::string::String {
    if token.is_null() || length <= 0 {
        return std::string::String::new();
    }
    let bytes = std::slice::from_raw_parts(token as *const u8, length as usize);
    std::string::String::from_utf8_lossy(bytes).into_owned()
}

/*
 * Macros to simplify reading of different kinds of fields.  These mirror the
 * READ_*_FIELD textual macros in readfuncs.c.  They hard-wire the local
 * variable names `local_node`, `token`, and `length` declared by READ_LOCALS.
 */

/*
 * Macros mirroring readfuncs.c's READ_*_FIELD.  Due to Rust macro hygiene, the
 * local variables (`local_node`, `token`, `length`) introduced by READ_LOCALS
 * cannot be referenced from a separate macro invocation.  We therefore pass the
 * three locals explicitly into each field macro.  To keep the reader functions
 * readable and 1:1 with the C, a single per-function `read_fields!` macro takes
 * the node var and a list of field operations.
 */

/* Allocate the node and declare the read locals. */
macro_rules! READ_LOCALS {
    ($nodetype:ty, $tag:ident) => {{
        makeNode!($nodetype, $tag)
    }};
}

/*
 * read_fields! expands a sequence of field operations against the explicitly
 * provided `local_node`, sharing the `token`/`length` locals it declares.
 */
macro_rules! read_fields {
    ($local_node:ident; $($op:tt)*) => {{
        #[allow(unused_assignments, unused_variables)]
        let mut token: *const c_char;
        #[allow(unused_assignments, unused_variables)]
        let mut length: c_int = 0;
        read_fields!(@inner $local_node, token, length; $($op)*);
        $local_node
    }};

    (@inner $ln:ident, $tok:ident, $len:ident; ) => {};

    /* INT(fld) */
    (@inner $ln:ident, $tok:ident, $len:ident; INT($f:ident) $($rest:tt)*) => {
        $tok = pg_strtok(&mut $len);
        $tok = pg_strtok(&mut $len);
        (*$ln).$f = atoi($tok) as _;
        read_fields!(@inner $ln, $tok, $len; $($rest)*);
    };
    /* UINT(fld) */
    (@inner $ln:ident, $tok:ident, $len:ident; UINT($f:ident) $($rest:tt)*) => {
        $tok = pg_strtok(&mut $len);
        $tok = pg_strtok(&mut $len);
        (*$ln).$f = atoui($tok) as _;
        read_fields!(@inner $ln, $tok, $len; $($rest)*);
    };
    /* OID(fld) */
    (@inner $ln:ident, $tok:ident, $len:ident; OID($f:ident) $($rest:tt)*) => {
        $tok = pg_strtok(&mut $len);
        $tok = pg_strtok(&mut $len);
        (*$ln).$f = atooid($tok);
        read_fields!(@inner $ln, $tok, $len; $($rest)*);
    };
    /* CHAR(fld) */
    (@inner $ln:ident, $tok:ident, $len:ident; CHAR($f:ident) $($rest:tt)*) => {
        $tok = pg_strtok(&mut $len);
        $tok = pg_strtok(&mut $len);
        (*$ln).$f = read_char($tok, $len);
        read_fields!(@inner $ln, $tok, $len; $($rest)*);
    };
    /* ENUM(fld, ty) */
    (@inner $ln:ident, $tok:ident, $len:ident; ENUM($f:ident, $ty:ty) $($rest:tt)*) => {
        $tok = pg_strtok(&mut $len);
        $tok = pg_strtok(&mut $len);
        (*$ln).$f = std::mem::transmute::<c_int, $ty>(atoi($tok));
        read_fields!(@inner $ln, $tok, $len; $($rest)*);
    };
    /* BOOL(fld) */
    (@inner $ln:ident, $tok:ident, $len:ident; BOOL($f:ident) $($rest:tt)*) => {
        $tok = pg_strtok(&mut $len);
        $tok = pg_strtok(&mut $len);
        (*$ln).$f = strtobool($tok);
        read_fields!(@inner $ln, $tok, $len; $($rest)*);
    };
    /* STRING(fld) */
    (@inner $ln:ident, $tok:ident, $len:ident; STRING($f:ident) $($rest:tt)*) => {
        $tok = pg_strtok(&mut $len);
        $tok = pg_strtok(&mut $len);
        (*$ln).$f = nullable_string($tok, $len);
        read_fields!(@inner $ln, $tok, $len; $($rest)*);
    };
    /* LOCATION(fld) */
    (@inner $ln:ident, $tok:ident, $len:ident; LOCATION($f:ident) $($rest:tt)*) => {
        $tok = pg_strtok(&mut $len);
        $tok = pg_strtok(&mut $len);
        (*$ln).$f = read_location($tok);
        read_fields!(@inner $ln, $tok, $len; $($rest)*);
    };
    /* NODE(fld) */
    (@inner $ln:ident, $tok:ident, $len:ident; NODE($f:ident) $($rest:tt)*) => {
        $tok = pg_strtok(&mut $len);
        let _ = $tok;
        (*$ln).$f = nodeRead(std::ptr::null(), 0) as *mut _;
        read_fields!(@inner $ln, $tok, $len; $($rest)*);
    };
    /* BITMAPSET(fld) */
    (@inner $ln:ident, $tok:ident, $len:ident; BITMAPSET($f:ident) $($rest:tt)*) => {
        $tok = pg_strtok(&mut $len);
        let _ = $tok;
        (*$ln).$f = _readBitmapset();
        read_fields!(@inner $ln, $tok, $len; $($rest)*);
    };
    /* RAW(stmt) - arbitrary statement (e.g. local_node->queryId = 0) */
    (@inner $ln:ident, $tok:ident, $len:ident; RAW($stmt:stmt) $($rest:tt)*) => {
        $stmt
        read_fields!(@inner $ln, $tok, $len; $($rest)*);
    };
}

/* ---- begin readfuncs.funcs.c (generated by gen_node_support.pl) ---- */
//
// A subset of the per-node-type _read<Tag>() reader functions generated into
// readfuncs.funcs.c are ported here (symmetric with outfuncs.rs).  The rest are
// still TODO(pg-port) and fall through to the parseNodeString error path.

use crate::nodes::primnodes::{
    Aggref, Alias, ArrayExpr, BooleanTest, CaseExpr, CaseWhen, CoalesceExpr, CoerceToDomain,
    CoerceViaIO, DistinctExpr, FieldSelect, FieldStore, FromExpr, FuncExpr, GroupingFunc, JoinExpr,
    MinMaxExpr, NullIfExpr, NullTest, OpExpr, Param, RangeVar, RelabelType, RowCompareExpr, RowExpr,
    SQLValueFunction, ScalarArrayOpExpr, SubLink, SubscriptingRef, TargetEntry, Var, WindowFunc,
};
use crate::nodes::primnodes::RangeTblRef;
use crate::nodes::parsenodes::{Query, SortGroupClause};

use crate::nodes::primnodes::{
    BoolTestType, CoercionForm, MinMaxOp, NullTestType, OverridingKind, ParamKind,
    SQLValueFunctionOp, SubLinkType, TableFuncType, VarReturningType,
};
use crate::nodes::nodes::{AggSplit, CmdType, LimitOption};
use crate::nodes::parsenodes::QuerySource;

unsafe fn _readAlias() -> *mut Alias {
    let local_node = READ_LOCALS!(Alias, T_Alias);
    read_fields!(local_node;
        STRING(aliasname)
        NODE(colnames)
    )
}

unsafe fn _readRangeVar() -> *mut RangeVar {
    let local_node = READ_LOCALS!(RangeVar, T_RangeVar);
    read_fields!(local_node;
        STRING(catalogname)
        STRING(schemaname)
        STRING(relname)
        BOOL(inh)
        CHAR(relpersistence)
        NODE(alias)
        LOCATION(location)
    )
}

unsafe fn _readTableFunc() -> *mut TableFunc {
    let local_node = READ_LOCALS!(TableFunc, T_TableFunc);
    read_fields!(local_node;
        ENUM(functype, TableFuncType)
        NODE(ns_uris)
        NODE(ns_names)
        NODE(docexpr)
        NODE(rowexpr)
        NODE(colnames)
        NODE(coltypes)
        NODE(coltypmods)
        NODE(colcollations)
        NODE(colexprs)
        NODE(coldefexprs)
        NODE(colvalexprs)
        NODE(passingvalexprs)
        BITMAPSET(notnulls)
        NODE(plan)
        INT(ordinalitycol)
        LOCATION(location)
    )
}

unsafe fn _readVar() -> *mut Var {
    let local_node = READ_LOCALS!(Var, T_Var);
    read_fields!(local_node;
        INT(varno)
        INT(varattno)
        OID(vartype)
        INT(vartypmod)
        OID(varcollid)
        BITMAPSET(varnullingrels)
        UINT(varlevelsup)
        ENUM(varreturningtype, VarReturningType)
        UINT(varnosyn)
        INT(varattnosyn)
        LOCATION(location)
    )
}

unsafe fn _readParam() -> *mut Param {
    let local_node = READ_LOCALS!(Param, T_Param);
    read_fields!(local_node;
        ENUM(paramkind, ParamKind)
        INT(paramid)
        OID(paramtype)
        INT(paramtypmod)
        OID(paramcollid)
        LOCATION(location)
    )
}

unsafe fn _readAggref() -> *mut Aggref {
    let local_node = READ_LOCALS!(Aggref, T_Aggref);
    read_fields!(local_node;
        OID(aggfnoid)
        OID(aggtype)
        OID(aggcollid)
        OID(inputcollid)
        OID(aggtranstype)
        NODE(aggargtypes)
        NODE(aggdirectargs)
        NODE(args)
        NODE(aggorder)
        NODE(aggdistinct)
        NODE(aggfilter)
        BOOL(aggstar)
        BOOL(aggvariadic)
        CHAR(aggkind)
        BOOL(aggpresorted)
        UINT(agglevelsup)
        ENUM(aggsplit, AggSplit)
        INT(aggno)
        INT(aggtransno)
        LOCATION(location)
    )
}

unsafe fn _readGroupingFunc() -> *mut GroupingFunc {
    let local_node = READ_LOCALS!(GroupingFunc, T_GroupingFunc);
    read_fields!(local_node;
        NODE(args)
        NODE(refs)
        NODE(cols)
        UINT(agglevelsup)
        LOCATION(location)
    )
}

unsafe fn _readWindowFunc() -> *mut WindowFunc {
    let local_node = READ_LOCALS!(WindowFunc, T_WindowFunc);
    read_fields!(local_node;
        OID(winfnoid)
        OID(wintype)
        OID(wincollid)
        OID(inputcollid)
        NODE(args)
        NODE(aggfilter)
        NODE(runCondition)
        UINT(winref)
        BOOL(winstar)
        BOOL(winagg)
        LOCATION(location)
    )
}

unsafe fn _readSubscriptingRef() -> *mut SubscriptingRef {
    let local_node = READ_LOCALS!(SubscriptingRef, T_SubscriptingRef);
    read_fields!(local_node;
        OID(refcontainertype)
        OID(refelemtype)
        OID(refrestype)
        INT(reftypmod)
        OID(refcollid)
        NODE(refupperindexpr)
        NODE(reflowerindexpr)
        NODE(refexpr)
        NODE(refassgnexpr)
    )
}

unsafe fn _readFuncExpr() -> *mut FuncExpr {
    let local_node = READ_LOCALS!(FuncExpr, T_FuncExpr);
    read_fields!(local_node;
        OID(funcid)
        OID(funcresulttype)
        BOOL(funcretset)
        BOOL(funcvariadic)
        ENUM(funcformat, CoercionForm)
        OID(funccollid)
        OID(inputcollid)
        NODE(args)
        LOCATION(location)
    )
}

unsafe fn _readOpExpr() -> *mut OpExpr {
    let local_node = READ_LOCALS!(OpExpr, T_OpExpr);
    read_fields!(local_node;
        OID(opno)
        OID(opfuncid)
        OID(opresulttype)
        BOOL(opretset)
        OID(opcollid)
        OID(inputcollid)
        NODE(args)
        LOCATION(location)
    )
}

unsafe fn _readDistinctExpr() -> *mut DistinctExpr {
    let local_node = READ_LOCALS!(DistinctExpr, T_DistinctExpr);
    read_fields!(local_node;
        OID(opno)
        OID(opfuncid)
        OID(opresulttype)
        BOOL(opretset)
        OID(opcollid)
        OID(inputcollid)
        NODE(args)
        LOCATION(location)
    )
}

unsafe fn _readNullIfExpr() -> *mut NullIfExpr {
    let local_node = READ_LOCALS!(NullIfExpr, T_NullIfExpr);
    read_fields!(local_node;
        OID(opno)
        OID(opfuncid)
        OID(opresulttype)
        BOOL(opretset)
        OID(opcollid)
        OID(inputcollid)
        NODE(args)
        LOCATION(location)
    )
}

unsafe fn _readScalarArrayOpExpr() -> *mut ScalarArrayOpExpr {
    let local_node = READ_LOCALS!(ScalarArrayOpExpr, T_ScalarArrayOpExpr);
    read_fields!(local_node;
        OID(opno)
        OID(opfuncid)
        OID(hashfuncid)
        OID(negfuncid)
        BOOL(useOr)
        OID(inputcollid)
        NODE(args)
        LOCATION(location)
    )
}

unsafe fn _readSubLink() -> *mut SubLink {
    let local_node = READ_LOCALS!(SubLink, T_SubLink);
    read_fields!(local_node;
        ENUM(subLinkType, SubLinkType)
        INT(subLinkId)
        NODE(testexpr)
        NODE(operName)
        NODE(subselect)
        LOCATION(location)
    )
}

unsafe fn _readFieldSelect() -> *mut FieldSelect {
    let local_node = READ_LOCALS!(FieldSelect, T_FieldSelect);
    read_fields!(local_node;
        NODE(arg)
        INT(fieldnum)
        OID(resulttype)
        INT(resulttypmod)
        OID(resultcollid)
    )
}

unsafe fn _readFieldStore() -> *mut FieldStore {
    let local_node = READ_LOCALS!(FieldStore, T_FieldStore);
    read_fields!(local_node;
        NODE(arg)
        NODE(newvals)
        NODE(fieldnums)
        OID(resulttype)
    )
}

unsafe fn _readRelabelType() -> *mut RelabelType {
    let local_node = READ_LOCALS!(RelabelType, T_RelabelType);
    read_fields!(local_node;
        NODE(arg)
        OID(resulttype)
        INT(resulttypmod)
        OID(resultcollid)
        ENUM(relabelformat, CoercionForm)
        LOCATION(location)
    )
}

unsafe fn _readCoerceViaIO() -> *mut CoerceViaIO {
    let local_node = READ_LOCALS!(CoerceViaIO, T_CoerceViaIO);
    read_fields!(local_node;
        NODE(arg)
        OID(resulttype)
        OID(resultcollid)
        ENUM(coerceformat, CoercionForm)
        LOCATION(location)
    )
}

unsafe fn _readCaseExpr() -> *mut CaseExpr {
    let local_node = READ_LOCALS!(CaseExpr, T_CaseExpr);
    read_fields!(local_node;
        OID(casetype)
        OID(casecollid)
        NODE(arg)
        NODE(args)
        NODE(defresult)
        LOCATION(location)
    )
}

unsafe fn _readCaseWhen() -> *mut CaseWhen {
    let local_node = READ_LOCALS!(CaseWhen, T_CaseWhen);
    read_fields!(local_node;
        NODE(expr)
        NODE(result)
        LOCATION(location)
    )
}

unsafe fn _readArrayExpr() -> *mut ArrayExpr {
    let local_node = READ_LOCALS!(ArrayExpr, T_ArrayExpr);
    read_fields!(local_node;
        OID(array_typeid)
        OID(array_collid)
        OID(element_typeid)
        NODE(elements)
        BOOL(multidims)
        LOCATION(list_start)
        LOCATION(list_end)
        LOCATION(location)
    )
}

unsafe fn _readRowExpr() -> *mut RowExpr {
    let local_node = READ_LOCALS!(RowExpr, T_RowExpr);
    read_fields!(local_node;
        NODE(args)
        OID(row_typeid)
        ENUM(row_format, CoercionForm)
        NODE(colnames)
        LOCATION(location)
    )
}

unsafe fn _readRowCompareExpr() -> *mut RowCompareExpr {
    let local_node = READ_LOCALS!(RowCompareExpr, T_RowCompareExpr);
    /* cmptype is a CompareType alias of c_int -> INT */
    read_fields!(local_node;
        INT(cmptype)
        NODE(opnos)
        NODE(opfamilies)
        NODE(inputcollids)
        NODE(largs)
        NODE(rargs)
    )
}

unsafe fn _readCoalesceExpr() -> *mut CoalesceExpr {
    let local_node = READ_LOCALS!(CoalesceExpr, T_CoalesceExpr);
    read_fields!(local_node;
        OID(coalescetype)
        OID(coalescecollid)
        NODE(args)
        LOCATION(location)
    )
}

unsafe fn _readMinMaxExpr() -> *mut MinMaxExpr {
    let local_node = READ_LOCALS!(MinMaxExpr, T_MinMaxExpr);
    read_fields!(local_node;
        OID(minmaxtype)
        OID(minmaxcollid)
        OID(inputcollid)
        ENUM(op, MinMaxOp)
        NODE(args)
        LOCATION(location)
    )
}

unsafe fn _readSQLValueFunction() -> *mut SQLValueFunction {
    let local_node = READ_LOCALS!(SQLValueFunction, T_SQLValueFunction);
    read_fields!(local_node;
        ENUM(op, SQLValueFunctionOp)
        OID(r#type)
        INT(typmod)
        LOCATION(location)
    )
}

unsafe fn _readNullTest() -> *mut NullTest {
    let local_node = READ_LOCALS!(NullTest, T_NullTest);
    read_fields!(local_node;
        NODE(arg)
        ENUM(nulltesttype, NullTestType)
        BOOL(argisrow)
        LOCATION(location)
    )
}

unsafe fn _readBooleanTest() -> *mut BooleanTest {
    let local_node = READ_LOCALS!(BooleanTest, T_BooleanTest);
    read_fields!(local_node;
        NODE(arg)
        ENUM(booltesttype, BoolTestType)
        LOCATION(location)
    )
}

unsafe fn _readCoerceToDomain() -> *mut CoerceToDomain {
    let local_node = READ_LOCALS!(CoerceToDomain, T_CoerceToDomain);
    read_fields!(local_node;
        NODE(arg)
        OID(resulttype)
        INT(resulttypmod)
        OID(resultcollid)
        ENUM(coercionformat, CoercionForm)
        LOCATION(location)
    )
}

unsafe fn _readTargetEntry() -> *mut TargetEntry {
    let local_node = READ_LOCALS!(TargetEntry, T_TargetEntry);
    read_fields!(local_node;
        NODE(expr)
        INT(resno)
        STRING(resname)
        UINT(ressortgroupref)
        OID(resorigtbl)
        INT(resorigcol)
        BOOL(resjunk)
    )
}

unsafe fn _readRangeTblRef() -> *mut RangeTblRef {
    let local_node = READ_LOCALS!(RangeTblRef, T_RangeTblRef);
    read_fields!(local_node;
        INT(rtindex)
    )
}

unsafe fn _readJoinExpr() -> *mut JoinExpr {
    let local_node = READ_LOCALS!(JoinExpr, T_JoinExpr);
    read_fields!(local_node;
        ENUM(jointype, JoinType)
        BOOL(isNatural)
        NODE(larg)
        NODE(rarg)
        NODE(usingClause)
        NODE(join_using_alias)
        NODE(quals)
        NODE(alias)
        INT(rtindex)
    )
}

unsafe fn _readFromExpr() -> *mut FromExpr {
    let local_node = READ_LOCALS!(FromExpr, T_FromExpr);
    read_fields!(local_node;
        NODE(fromlist)
        NODE(quals)
    )
}

unsafe fn _readSortGroupClause() -> *mut SortGroupClause {
    let local_node = READ_LOCALS!(SortGroupClause, T_SortGroupClause);
    read_fields!(local_node;
        UINT(tleSortGroupRef)
        OID(eqop)
        OID(sortop)
        BOOL(reverse_sort)
        BOOL(nulls_first)
        BOOL(hashable)
    )
}

unsafe fn _readQuery() -> *mut Query {
    let local_node = READ_LOCALS!(Query, T_Query);
    read_fields!(local_node;
        ENUM(commandType, CmdType)
        ENUM(querySource, QuerySource)
        RAW((*local_node).queryId = 0)
        BOOL(canSetTag)
        NODE(utilityStmt)
        INT(resultRelation)
        BOOL(hasAggs)
        BOOL(hasWindowFuncs)
        BOOL(hasTargetSRFs)
        BOOL(hasSubLinks)
        BOOL(hasDistinctOn)
        BOOL(hasRecursive)
        BOOL(hasModifyingCTE)
        BOOL(hasForUpdate)
        BOOL(hasRowSecurity)
        BOOL(hasGroupRTE)
        BOOL(isReturn)
        NODE(cteList)
        NODE(rtable)
        NODE(rteperminfos)
        NODE(jointree)
        NODE(mergeActionList)
        INT(mergeTargetRelation)
        NODE(mergeJoinCondition)
        NODE(targetList)
        ENUM(r#override, OverridingKind)
        NODE(onConflict)
        STRING(returningOldAlias)
        STRING(returningNewAlias)
        NODE(returningList)
        NODE(groupClause)
        BOOL(groupDistinct)
        NODE(groupingSets)
        NODE(havingQual)
        NODE(windowClause)
        NODE(distinctClause)
        NODE(sortClause)
        NODE(limitOffset)
        NODE(limitCount)
        ENUM(limitOption, LimitOption)
        NODE(rowMarks)
        NODE(setOperations)
        NODE(constraintDeps)
        NODE(withCheckOptions)
        LOCATION(stmt_location)
        LOCATION(stmt_len)
    )
}

/* ---- end readfuncs.funcs.c ---- */


/*
 * Support functions for nodes with custom_read_write attribute or
 * special_read_write attribute
 */

unsafe fn _readConst() -> *mut Const {
    let local_node: *mut Const = makeNode!(Const, T_Const);
    let mut token: *const c_char;
    let mut length: c_int = 0;

    /* READ_OID_FIELD(consttype) */
    token = pg_strtok(&mut length); /* skip :consttype */
    token = pg_strtok(&mut length); /* get field value */
    (*local_node).consttype = atooid(token);
    /* READ_INT_FIELD(consttypmod) */
    token = pg_strtok(&mut length);
    token = pg_strtok(&mut length);
    (*local_node).consttypmod = atoi(token);
    /* READ_OID_FIELD(constcollid) */
    token = pg_strtok(&mut length);
    token = pg_strtok(&mut length);
    (*local_node).constcollid = atooid(token);
    /* READ_INT_FIELD(constlen) */
    token = pg_strtok(&mut length);
    token = pg_strtok(&mut length);
    (*local_node).constlen = atoi(token);
    /* READ_BOOL_FIELD(constbyval) */
    token = pg_strtok(&mut length);
    token = pg_strtok(&mut length);
    (*local_node).constbyval = strtobool(token);
    /* READ_BOOL_FIELD(constisnull) */
    token = pg_strtok(&mut length);
    token = pg_strtok(&mut length);
    (*local_node).constisnull = strtobool(token);
    /* READ_LOCATION_FIELD(location) */
    token = pg_strtok(&mut length);
    token = pg_strtok(&mut length);
    (*local_node).location = read_location(token);

    token = pg_strtok(&mut length); /* skip :constvalue */
    if (*local_node).constisnull {
        token = pg_strtok(&mut length); /* skip "<>" */
        let _ = token;
    } else {
        (*local_node).constvalue = readDatum((*local_node).constbyval);
    }

    local_node
}

unsafe fn _readBoolExpr() -> *mut BoolExpr {
    let local_node: *mut BoolExpr = makeNode!(BoolExpr, T_BoolExpr);
    let mut token: *const c_char;
    let mut length: c_int = 0;

    /* do-it-yourself enum representation */
    token = pg_strtok(&mut length); /* skip :boolop */
    token = pg_strtok(&mut length); /* get field value */
    if length == 3 && strncmp(token, c"and".as_ptr(), 3) == 0 {
        (*local_node).boolop = AND_EXPR;
    } else if length == 2 && strncmp(token, c"or".as_ptr(), 2) == 0 {
        (*local_node).boolop = OR_EXPR;
    } else if length == 3 && strncmp(token, c"not".as_ptr(), 3) == 0 {
        (*local_node).boolop = NOT_EXPR;
    } else {
        elog!(ERROR, "unrecognized boolop \"{}\"", show_token(token, length));
    }

    /* READ_NODE_FIELD(args) */
    token = pg_strtok(&mut length); /* skip :args */
    let _ = token;
    (*local_node).args = nodeRead(std::ptr::null(), 0) as *mut _;
    /* READ_LOCATION_FIELD(location) */
    token = pg_strtok(&mut length);
    token = pg_strtok(&mut length);
    (*local_node).location = read_location(token);

    local_node
}

unsafe fn _readA_Const() -> *mut A_Const {
    let local_node: *mut A_Const = makeNode!(A_Const, T_A_Const);
    let mut token: *const c_char;
    let mut length: c_int = 0;

    /* We expect either NULL or :val here */
    token = pg_strtok(&mut length);
    if length == 4 && strncmp(token, c"NULL".as_ptr(), 4) == 0 {
        (*local_node).isnull = true;
    } else {
        let tmp = nodeRead(std::ptr::null(), 0) as *mut Node;

        /* To forestall valgrind complaints, copy only the valid data */
        match nodeTag(tmp) {
            NodeTag::T_Integer => {
                memcpy(
                    &mut (*local_node).val as *mut _ as *mut c_void,
                    tmp as *const c_void,
                    std::mem::size_of::<Integer>(),
                );
            }
            NodeTag::T_Float => {
                memcpy(
                    &mut (*local_node).val as *mut _ as *mut c_void,
                    tmp as *const c_void,
                    std::mem::size_of::<Float>(),
                );
            }
            NodeTag::T_Boolean => {
                memcpy(
                    &mut (*local_node).val as *mut _ as *mut c_void,
                    tmp as *const c_void,
                    std::mem::size_of::<Boolean>(),
                );
            }
            NodeTag::T_String => {
                memcpy(
                    &mut (*local_node).val as *mut _ as *mut c_void,
                    tmp as *const c_void,
                    std::mem::size_of::<PgString>(),
                );
            }
            NodeTag::T_BitString => {
                memcpy(
                    &mut (*local_node).val as *mut _ as *mut c_void,
                    tmp as *const c_void,
                    std::mem::size_of::<BitString>(),
                );
            }
            _ => {
                elog!(ERROR, "unrecognized node type: {}", nodeTag(tmp) as c_int);
            }
        }
    }

    /* READ_LOCATION_FIELD(location) */
    token = pg_strtok(&mut length);
    token = pg_strtok(&mut length);
    (*local_node).location = read_location(token);

    local_node
}

unsafe fn _readRangeTblEntry() -> *mut RangeTblEntry {
    let local_node: *mut RangeTblEntry = makeNode!(RangeTblEntry, T_RangeTblEntry);
    let mut token: *const c_char;
    let mut length: c_int = 0;

    /* READ_NODE_FIELD(alias) */
    token = pg_strtok(&mut length);
    let _ = token;
    (*local_node).alias = nodeRead(std::ptr::null(), 0) as *mut _;
    /* READ_NODE_FIELD(eref) */
    token = pg_strtok(&mut length);
    let _ = token;
    (*local_node).eref = nodeRead(std::ptr::null(), 0) as *mut _;
    /* READ_ENUM_FIELD(rtekind, RTEKind) */
    token = pg_strtok(&mut length);
    token = pg_strtok(&mut length);
    (*local_node).rtekind = std::mem::transmute::<c_int, RTEKind>(atoi(token));

    match (*local_node).rtekind {
        RTE_RELATION => {
            /* READ_OID_FIELD(relid) */
            token = pg_strtok(&mut length);
            token = pg_strtok(&mut length);
            (*local_node).relid = atooid(token);
            /* READ_BOOL_FIELD(inh) */
            token = pg_strtok(&mut length);
            token = pg_strtok(&mut length);
            (*local_node).inh = strtobool(token);
            /* READ_CHAR_FIELD(relkind) */
            token = pg_strtok(&mut length);
            token = pg_strtok(&mut length);
            (*local_node).relkind = read_char(token, length);
            /* READ_INT_FIELD(rellockmode) */
            token = pg_strtok(&mut length);
            token = pg_strtok(&mut length);
            (*local_node).rellockmode = atoi(token);
            /* READ_UINT_FIELD(perminfoindex) */
            token = pg_strtok(&mut length);
            token = pg_strtok(&mut length);
            (*local_node).perminfoindex = atoui(token);
            /* READ_NODE_FIELD(tablesample) */
            token = pg_strtok(&mut length);
            (*local_node).tablesample = nodeRead(std::ptr::null(), 0) as *mut _;
        }
        RTE_SUBQUERY => {
            /* READ_NODE_FIELD(subquery) */
            token = pg_strtok(&mut length);
            (*local_node).subquery = nodeRead(std::ptr::null(), 0) as *mut _;
            /* READ_BOOL_FIELD(security_barrier) */
            token = pg_strtok(&mut length);
            token = pg_strtok(&mut length);
            (*local_node).security_barrier = strtobool(token);
            /* we re-use these RELATION fields, too: */
            /* READ_OID_FIELD(relid) */
            token = pg_strtok(&mut length);
            token = pg_strtok(&mut length);
            (*local_node).relid = atooid(token);
            /* READ_BOOL_FIELD(inh) */
            token = pg_strtok(&mut length);
            token = pg_strtok(&mut length);
            (*local_node).inh = strtobool(token);
            /* READ_CHAR_FIELD(relkind) */
            token = pg_strtok(&mut length);
            token = pg_strtok(&mut length);
            (*local_node).relkind = read_char(token, length);
            /* READ_INT_FIELD(rellockmode) */
            token = pg_strtok(&mut length);
            token = pg_strtok(&mut length);
            (*local_node).rellockmode = atoi(token);
            /* READ_UINT_FIELD(perminfoindex) */
            token = pg_strtok(&mut length);
            token = pg_strtok(&mut length);
            (*local_node).perminfoindex = atoui(token);
        }
        RTE_JOIN => {
            /* READ_ENUM_FIELD(jointype, JoinType) */
            token = pg_strtok(&mut length);
            token = pg_strtok(&mut length);
            (*local_node).jointype = std::mem::transmute::<c_int, JoinType>(atoi(token));
            /* READ_INT_FIELD(joinmergedcols) */
            token = pg_strtok(&mut length);
            token = pg_strtok(&mut length);
            (*local_node).joinmergedcols = atoi(token);
            /* READ_NODE_FIELD(joinaliasvars) */
            token = pg_strtok(&mut length);
            (*local_node).joinaliasvars = nodeRead(std::ptr::null(), 0) as *mut _;
            /* READ_NODE_FIELD(joinleftcols) */
            token = pg_strtok(&mut length);
            (*local_node).joinleftcols = nodeRead(std::ptr::null(), 0) as *mut _;
            /* READ_NODE_FIELD(joinrightcols) */
            token = pg_strtok(&mut length);
            (*local_node).joinrightcols = nodeRead(std::ptr::null(), 0) as *mut _;
            /* READ_NODE_FIELD(join_using_alias) */
            token = pg_strtok(&mut length);
            (*local_node).join_using_alias = nodeRead(std::ptr::null(), 0) as *mut _;
        }
        RTE_FUNCTION => {
            /* READ_NODE_FIELD(functions) */
            token = pg_strtok(&mut length);
            (*local_node).functions = nodeRead(std::ptr::null(), 0) as *mut _;
            /* READ_BOOL_FIELD(funcordinality) */
            token = pg_strtok(&mut length);
            token = pg_strtok(&mut length);
            (*local_node).funcordinality = strtobool(token);
        }
        RTE_TABLEFUNC => {
            /* READ_NODE_FIELD(tablefunc) */
            token = pg_strtok(&mut length);
            (*local_node).tablefunc = nodeRead(std::ptr::null(), 0) as *mut _;
            /* The RTE must have a copy of the column type info, if any */
            if !(*local_node).tablefunc.is_null() {
                let tf: *mut TableFunc = (*local_node).tablefunc;
                (*local_node).coltypes = (*tf).coltypes;
                (*local_node).coltypmods = (*tf).coltypmods;
                (*local_node).colcollations = (*tf).colcollations;
            }
        }
        RTE_VALUES => {
            /* READ_NODE_FIELD(values_lists) */
            token = pg_strtok(&mut length);
            (*local_node).values_lists = nodeRead(std::ptr::null(), 0) as *mut _;
            /* READ_NODE_FIELD(coltypes) */
            token = pg_strtok(&mut length);
            (*local_node).coltypes = nodeRead(std::ptr::null(), 0) as *mut _;
            /* READ_NODE_FIELD(coltypmods) */
            token = pg_strtok(&mut length);
            (*local_node).coltypmods = nodeRead(std::ptr::null(), 0) as *mut _;
            /* READ_NODE_FIELD(colcollations) */
            token = pg_strtok(&mut length);
            (*local_node).colcollations = nodeRead(std::ptr::null(), 0) as *mut _;
        }
        RTE_CTE => {
            /* READ_STRING_FIELD(ctename) */
            token = pg_strtok(&mut length);
            token = pg_strtok(&mut length);
            (*local_node).ctename = nullable_string(token, length);
            /* READ_UINT_FIELD(ctelevelsup) */
            token = pg_strtok(&mut length);
            token = pg_strtok(&mut length);
            (*local_node).ctelevelsup = atoui(token);
            /* READ_BOOL_FIELD(self_reference) */
            token = pg_strtok(&mut length);
            token = pg_strtok(&mut length);
            (*local_node).self_reference = strtobool(token);
            /* READ_NODE_FIELD(coltypes) */
            token = pg_strtok(&mut length);
            (*local_node).coltypes = nodeRead(std::ptr::null(), 0) as *mut _;
            /* READ_NODE_FIELD(coltypmods) */
            token = pg_strtok(&mut length);
            (*local_node).coltypmods = nodeRead(std::ptr::null(), 0) as *mut _;
            /* READ_NODE_FIELD(colcollations) */
            token = pg_strtok(&mut length);
            (*local_node).colcollations = nodeRead(std::ptr::null(), 0) as *mut _;
        }
        RTE_NAMEDTUPLESTORE => {
            /* READ_STRING_FIELD(enrname) */
            token = pg_strtok(&mut length);
            token = pg_strtok(&mut length);
            (*local_node).enrname = nullable_string(token, length);
            /* READ_FLOAT_FIELD(enrtuples) */
            token = pg_strtok(&mut length);
            token = pg_strtok(&mut length);
            (*local_node).enrtuples = atof(token);
            /* READ_NODE_FIELD(coltypes) */
            token = pg_strtok(&mut length);
            (*local_node).coltypes = nodeRead(std::ptr::null(), 0) as *mut _;
            /* READ_NODE_FIELD(coltypmods) */
            token = pg_strtok(&mut length);
            (*local_node).coltypmods = nodeRead(std::ptr::null(), 0) as *mut _;
            /* READ_NODE_FIELD(colcollations) */
            token = pg_strtok(&mut length);
            (*local_node).colcollations = nodeRead(std::ptr::null(), 0) as *mut _;
            /* we re-use these RELATION fields, too: */
            /* READ_OID_FIELD(relid) */
            token = pg_strtok(&mut length);
            token = pg_strtok(&mut length);
            (*local_node).relid = atooid(token);
        }
        RTE_RESULT => {
            /* no extra fields */
        }
        RTE_GROUP => {
            /* READ_NODE_FIELD(groupexprs) */
            token = pg_strtok(&mut length);
            (*local_node).groupexprs = nodeRead(std::ptr::null(), 0) as *mut _;
        }
        #[allow(unreachable_patterns)]
        _ => {
            elog!(ERROR, "unrecognized RTE kind: {}", (*local_node).rtekind as c_int);
        }
    }

    /* READ_BOOL_FIELD(lateral) */
    token = pg_strtok(&mut length);
    token = pg_strtok(&mut length);
    (*local_node).lateral = strtobool(token);
    /* READ_BOOL_FIELD(inFromCl) */
    token = pg_strtok(&mut length);
    token = pg_strtok(&mut length);
    (*local_node).inFromCl = strtobool(token);
    /* READ_NODE_FIELD(securityQuals) */
    token = pg_strtok(&mut length);
    let _ = token;
    (*local_node).securityQuals = nodeRead(std::ptr::null(), 0) as *mut _;

    local_node
}

unsafe fn _readA_Expr() -> *mut A_Expr {
    let local_node: *mut A_Expr = makeNode!(A_Expr, T_A_Expr);
    let mut token: *const c_char;
    let mut length: c_int = 0;

    token = pg_strtok(&mut length);

    if length == 3 && strncmp(token, c"ANY".as_ptr(), 3) == 0 {
        (*local_node).kind = AEXPR_OP_ANY;
        token = pg_strtok(&mut length);
        (*local_node).name = nodeRead(std::ptr::null(), 0) as *mut _;
    } else if length == 3 && strncmp(token, c"ALL".as_ptr(), 3) == 0 {
        (*local_node).kind = AEXPR_OP_ALL;
        token = pg_strtok(&mut length);
        (*local_node).name = nodeRead(std::ptr::null(), 0) as *mut _;
    } else if length == 8 && strncmp(token, c"DISTINCT".as_ptr(), 8) == 0 {
        (*local_node).kind = AEXPR_DISTINCT;
        token = pg_strtok(&mut length);
        (*local_node).name = nodeRead(std::ptr::null(), 0) as *mut _;
    } else if length == 12 && strncmp(token, c"NOT_DISTINCT".as_ptr(), 12) == 0 {
        (*local_node).kind = AEXPR_NOT_DISTINCT;
        token = pg_strtok(&mut length);
        (*local_node).name = nodeRead(std::ptr::null(), 0) as *mut _;
    } else if length == 6 && strncmp(token, c"NULLIF".as_ptr(), 6) == 0 {
        (*local_node).kind = AEXPR_NULLIF;
        token = pg_strtok(&mut length);
        (*local_node).name = nodeRead(std::ptr::null(), 0) as *mut _;
    } else if length == 2 && strncmp(token, c"IN".as_ptr(), 2) == 0 {
        (*local_node).kind = AEXPR_IN;
        token = pg_strtok(&mut length);
        (*local_node).name = nodeRead(std::ptr::null(), 0) as *mut _;
    } else if length == 4 && strncmp(token, c"LIKE".as_ptr(), 4) == 0 {
        (*local_node).kind = AEXPR_LIKE;
        token = pg_strtok(&mut length);
        (*local_node).name = nodeRead(std::ptr::null(), 0) as *mut _;
    } else if length == 5 && strncmp(token, c"ILIKE".as_ptr(), 5) == 0 {
        (*local_node).kind = AEXPR_ILIKE;
        token = pg_strtok(&mut length);
        (*local_node).name = nodeRead(std::ptr::null(), 0) as *mut _;
    } else if length == 7 && strncmp(token, c"SIMILAR".as_ptr(), 7) == 0 {
        (*local_node).kind = AEXPR_SIMILAR;
        token = pg_strtok(&mut length);
        (*local_node).name = nodeRead(std::ptr::null(), 0) as *mut _;
    } else if length == 7 && strncmp(token, c"BETWEEN".as_ptr(), 7) == 0 {
        (*local_node).kind = AEXPR_BETWEEN;
        token = pg_strtok(&mut length);
        (*local_node).name = nodeRead(std::ptr::null(), 0) as *mut _;
    } else if length == 11 && strncmp(token, c"NOT_BETWEEN".as_ptr(), 11) == 0 {
        (*local_node).kind = AEXPR_NOT_BETWEEN;
        token = pg_strtok(&mut length);
        (*local_node).name = nodeRead(std::ptr::null(), 0) as *mut _;
    } else if length == 11 && strncmp(token, c"BETWEEN_SYM".as_ptr(), 11) == 0 {
        (*local_node).kind = AEXPR_BETWEEN_SYM;
        token = pg_strtok(&mut length);
        (*local_node).name = nodeRead(std::ptr::null(), 0) as *mut _;
    } else if length == 15 && strncmp(token, c"NOT_BETWEEN_SYM".as_ptr(), 15) == 0 {
        (*local_node).kind = AEXPR_NOT_BETWEEN_SYM;
        token = pg_strtok(&mut length);
        (*local_node).name = nodeRead(std::ptr::null(), 0) as *mut _;
    } else if length == 5 && strncmp(token, c":name".as_ptr(), 5) == 0 {
        (*local_node).kind = AEXPR_OP;
        (*local_node).name = nodeRead(std::ptr::null(), 0) as *mut _;
    } else {
        elog!(ERROR, "unrecognized A_Expr kind: \"{}\"", show_token(token, length));
    }

    /* READ_NODE_FIELD(lexpr) */
    token = pg_strtok(&mut length);
    (*local_node).lexpr = nodeRead(std::ptr::null(), 0) as *mut _;
    /* READ_NODE_FIELD(rexpr) */
    token = pg_strtok(&mut length);
    (*local_node).rexpr = nodeRead(std::ptr::null(), 0) as *mut _;
    /* READ_LOCATION_FIELD(rexpr_list_start) */
    token = pg_strtok(&mut length);
    token = pg_strtok(&mut length);
    (*local_node).rexpr_list_start = read_location(token);
    /* READ_LOCATION_FIELD(rexpr_list_end) */
    token = pg_strtok(&mut length);
    token = pg_strtok(&mut length);
    (*local_node).rexpr_list_end = read_location(token);
    /* READ_LOCATION_FIELD(location) */
    token = pg_strtok(&mut length);
    token = pg_strtok(&mut length);
    (*local_node).location = read_location(token);

    local_node
}

unsafe fn _readExtensibleNode() -> *mut ExtensibleNode {
    let methods: *const ExtensibleNodeMethods;
    let local_node: *mut ExtensibleNode;
    let extnodename: *const c_char;

    let mut token: *const c_char;
    let mut length: c_int = 0;

    token = pg_strtok(&mut length); /* skip :extnodename */
    token = pg_strtok(&mut length); /* get extnodename */

    extnodename = nullable_string(token, length);
    if extnodename.is_null() {
        elog!(ERROR, "extnodename has to be supplied");
    }
    methods = GetExtensibleNodeMethods(extnodename, false);

    local_node = newNode((*methods).node_size, NodeTag::T_ExtensibleNode) as *mut ExtensibleNode;
    (*local_node).extnodename = extnodename;

    /* deserialize the private fields */
    ((*methods).nodeRead.unwrap())(local_node);

    local_node
}

/*
 * Read a char field (ie, one ascii character).
 * Avoids overhead of calling debackslash() for one char.
 *   local_node->fldname =
 *     (length == 0) ? '\0' : (token[0] == '\\' ? token[1] : token[0])
 */
#[inline]
unsafe fn read_char(token: *const c_char, length: c_int) -> c_char {
    if length == 0 {
        0
    } else if *token == b'\\' as c_char {
        *token.add(1)
    } else {
        *token
    }
}

/*
 * Read a parse location field (and possibly throw away the value).
 *
 * With DEBUG_NODE_TESTS_ENABLED, restore the value when restore_location_fields
 * is set; otherwise always set the field to "unknown" (-1).
 */
#[inline]
unsafe fn read_location(token: *const c_char) -> ParseLoc {
    #[cfg(debug_assertions)]
    {
        if restore_location_fields {
            atoi(token) as ParseLoc
        } else {
            -1
        }
    }
    #[cfg(not(debug_assertions))]
    {
        let _ = token; /* in case not used elsewhere */
        -1 /* set field to "unknown" */
    }
}

/*
 * parseNodeString
 *
 * Given a character string representing a node tree, parseNodeString creates
 * the internal node structure.
 *
 * The string to be read must already have been loaded into pg_strtok().
 */
pub unsafe fn parseNodeString() -> *mut Node {
    let mut length: c_int = 0;
    let token: *const c_char;

    /* Guard against stack overflow due to overly complex expressions */
    check_stack_depth();

    token = pg_strtok(&mut length);

    // MATCH(tokname, namelen) == (length == namelen && memcmp(token, tokname, namelen) == 0)
    let matches = |tokname: &std::ffi::CStr, namelen: c_int| -> bool {
        length == namelen
            && memcmp(token as *const c_void, tokname.as_ptr() as *const c_void, namelen as usize)
                == 0
    };

    // The bulk of the dispatch table lives in the generated readfuncs.switch.c
    // (produced by gen_node_support.pl), which calls one _read<Tag>() per node
    // type.  That generated file is not yet ported; only the hand-written
    // custom_read_write / special_read_write reader functions are dispatched
    // here.  Everything else is TODO(pg-port).
    /* ---- begin readfuncs.switch.c ---- */
    if matches(c"ALIAS", 5) {
        return _readAlias() as *mut Node;
    } else if matches(c"RANGEVAR", 8) {
        return _readRangeVar() as *mut Node;
    } else if matches(c"TABLEFUNC", 9) {
        return _readTableFunc() as *mut Node;
    } else if matches(c"VAR", 3) {
        return _readVar() as *mut Node;
    } else if matches(c"CONST", 5) {
        return _readConst() as *mut Node;
    } else if matches(c"PARAM", 5) {
        return _readParam() as *mut Node;
    } else if matches(c"AGGREF", 6) {
        return _readAggref() as *mut Node;
    } else if matches(c"GROUPINGFUNC", 12) {
        return _readGroupingFunc() as *mut Node;
    } else if matches(c"WINDOWFUNC", 10) {
        return _readWindowFunc() as *mut Node;
    } else if matches(c"SUBSCRIPTINGREF", 15) {
        return _readSubscriptingRef() as *mut Node;
    } else if matches(c"FUNCEXPR", 8) {
        return _readFuncExpr() as *mut Node;
    } else if matches(c"OPEXPR", 6) {
        return _readOpExpr() as *mut Node;
    } else if matches(c"DISTINCTEXPR", 12) {
        return _readDistinctExpr() as *mut Node;
    } else if matches(c"NULLIFEXPR", 10) {
        return _readNullIfExpr() as *mut Node;
    } else if matches(c"SCALARARRAYOPEXPR", 17) {
        return _readScalarArrayOpExpr() as *mut Node;
    } else if matches(c"BOOLEXPR", 8) {
        return _readBoolExpr() as *mut Node;
    } else if matches(c"SUBLINK", 7) {
        return _readSubLink() as *mut Node;
    } else if matches(c"FIELDSELECT", 11) {
        return _readFieldSelect() as *mut Node;
    } else if matches(c"FIELDSTORE", 10) {
        return _readFieldStore() as *mut Node;
    } else if matches(c"RELABELTYPE", 11) {
        return _readRelabelType() as *mut Node;
    } else if matches(c"COERCEVIAIO", 11) {
        return _readCoerceViaIO() as *mut Node;
    } else if matches(c"CASEEXPR", 8) {
        return _readCaseExpr() as *mut Node;
    } else if matches(c"CASEWHEN", 8) {
        return _readCaseWhen() as *mut Node;
    } else if matches(c"ARRAYEXPR", 9) {
        return _readArrayExpr() as *mut Node;
    } else if matches(c"ROWEXPR", 7) {
        return _readRowExpr() as *mut Node;
    } else if matches(c"ROWCOMPAREEXPR", 14) {
        return _readRowCompareExpr() as *mut Node;
    } else if matches(c"COALESCEEXPR", 12) {
        return _readCoalesceExpr() as *mut Node;
    } else if matches(c"MINMAXEXPR", 10) {
        return _readMinMaxExpr() as *mut Node;
    } else if matches(c"SQLVALUEFUNCTION", 16) {
        return _readSQLValueFunction() as *mut Node;
    } else if matches(c"NULLTEST", 8) {
        return _readNullTest() as *mut Node;
    } else if matches(c"BOOLEANTEST", 11) {
        return _readBooleanTest() as *mut Node;
    } else if matches(c"COERCETODOMAIN", 14) {
        return _readCoerceToDomain() as *mut Node;
    } else if matches(c"TARGETENTRY", 11) {
        return _readTargetEntry() as *mut Node;
    } else if matches(c"RANGETBLREF", 11) {
        return _readRangeTblRef() as *mut Node;
    } else if matches(c"JOINEXPR", 8) {
        return _readJoinExpr() as *mut Node;
    } else if matches(c"FROMEXPR", 8) {
        return _readFromExpr() as *mut Node;
    } else if matches(c"QUERY", 5) {
        return _readQuery() as *mut Node;
    } else if matches(c"A_CONST", 7) {
        return _readA_Const() as *mut Node;
    } else if matches(c"RANGETBLENTRY", 13) {
        return _readRangeTblEntry() as *mut Node;
    } else if matches(c"SORTGROUPCLAUSE", 15) {
        return _readSortGroupClause() as *mut Node;
    } else if matches(c"A_EXPR", 6) {
        return _readA_Expr() as *mut Node;
    } else if matches(c"EXTENSIBLENODE", 14) {
        return _readExtensibleNode() as *mut Node;
    }
    /* ---- end readfuncs.switch.c ---- */

    elog!(ERROR, "badly formatted node string \"{}\"...", show_token(token, length.min(32)));
    #[allow(unreachable_code)]
    {
        std::ptr::null_mut() /* keep compiler quiet */
    }
}

/*
 * readDatum
 *
 * Given a string representation of a constant, recreate the appropriate
 * Datum.  The string representation embeds length info, but not byValue,
 * so we must be told that.
 */
pub unsafe fn readDatum(typbyval: bool) -> Datum {
    let length: Size;
    let i: Size;
    let mut tokenLength: c_int = 0;
    let mut token: *const c_char;
    let res: Datum;
    let s: *mut c_char;

    /*
     * read the actual length of the value
     */
    token = pg_strtok(&mut tokenLength);
    length = atoui(token) as Size;

    token = pg_strtok(&mut tokenLength); /* read the '[' */
    if token.is_null() || *token != b'[' as c_char {
        elog!(
            ERROR,
            "expected \"[\" to start datum, but got \"{}\"; length = {}",
            if token.is_null() {
                "[NULL]".to_string()
            } else {
                show_token(token, tokenLength)
            },
            length
        );
    }

    if typbyval {
        if length > std::mem::size_of::<Datum>() as Size {
            elog!(ERROR, "byval datum but length = {}", length);
        }
        res = 0 as Datum;
        s = &res as *const Datum as *mut c_char;
        let mut i: Size = 0;
        while i < std::mem::size_of::<Datum>() as Size {
            token = pg_strtok(&mut tokenLength);
            *s.add(i as usize) = atoi(token) as c_char;
            i += 1;
        }
        let _ = i;
    } else if (length as isize) <= 0 {
        res = 0 as Datum; /* (Datum) NULL */
    } else {
        s = palloc(length) as *mut c_char;
        let mut i: Size = 0;
        while i < length {
            token = pg_strtok(&mut tokenLength);
            *s.add(i as usize) = atoi(token) as c_char;
            i += 1;
        }
        let _ = i;
        res = PointerGetDatum(s as *const c_void);
    }
    let _ = i;

    token = pg_strtok(&mut tokenLength); /* read the ']' */
    if token.is_null() || *token != b']' as c_char {
        elog!(
            ERROR,
            "expected \"]\" to end datum, but got \"{}\"; length = {}",
            if token.is_null() {
                "[NULL]".to_string()
            } else {
                show_token(token, tokenLength)
            },
            length
        );
    }

    res
}

/*
 * common implementation for scalar-array-reading functions
 *
 * The data format is either "<>" for a NULL pointer (in which case numCols
 * is ignored) or "(item item item)" where the number of items must equal
 * numCols.  The convfunc must be okay with stopping at whitespace or a
 * right parenthesis, since pg_strtok won't null-terminate the token.
 *
 * In C this is the READ_SCALAR_ARRAY macro; here it is a generic helper.
 */
unsafe fn read_scalar_array<T: Copy>(
    numCols: c_int,
    convfunc: unsafe fn(*const c_char) -> T,
) -> *mut T {
    let mut length: c_int = 0;
    let mut token: *const c_char;

    token = pg_strtok(&mut length);
    if token.is_null() {
        elog!(ERROR, "incomplete scalar array");
    }
    if length == 0 {
        return std::ptr::null_mut(); /* it was "<>", so return NULL pointer */
    }
    if length != 1 || *token != b'(' as c_char {
        elog!(ERROR, "unrecognized token: \"{}\"", show_token(token, length));
    }
    let vals = palloc(numCols as Size * std::mem::size_of::<T>() as Size) as *mut T;
    for i in 0..numCols {
        token = pg_strtok(&mut length);
        if token.is_null() || *token == b')' as c_char {
            elog!(ERROR, "incomplete scalar array");
        }
        *vals.add(i as usize) = convfunc(token);
    }
    token = pg_strtok(&mut length);
    if token.is_null() || length != 1 || *token != b')' as c_char {
        elog!(ERROR, "incomplete scalar array");
    }
    vals
}

/*
 * Note: these functions are exported in nodes.h for possible use by
 * extensions, so don't mess too much with their names or API.
 */
pub unsafe fn readAttrNumberCols(numCols: c_int) -> *mut i16 {
    unsafe fn conv(token: *const c_char) -> i16 {
        atoi(token) as i16
    }
    read_scalar_array::<i16>(numCols, conv)
}

pub unsafe fn readOidCols(numCols: c_int) -> *mut Oid {
    unsafe fn conv(token: *const c_char) -> Oid {
        atooid(token)
    }
    read_scalar_array::<Oid>(numCols, conv)
}

/* outfuncs.c has writeIndexCols, but we don't yet need that here */
/* READ_SCALAR_ARRAY(readIndexCols, Index, atoui) */

pub unsafe fn readIntCols(numCols: c_int) -> *mut c_int {
    unsafe fn conv(token: *const c_char) -> c_int {
        atoi(token)
    }
    read_scalar_array::<c_int>(numCols, conv)
}

pub unsafe fn readBoolCols(numCols: c_int) -> *mut bool {
    unsafe fn conv(token: *const c_char) -> bool {
        strtobool(token)
    }
    read_scalar_array::<bool>(numCols, conv)
}

// The following helpers exist to satisfy `atol`/`strtoi64`/`strtou64` usage by
// the not-yet-ported generated readers (READ_LONG_FIELD/READ_INT64_FIELD/
// READ_UINT64_FIELD); keep references so they remain available.
#[allow(dead_code)]
unsafe fn _readfuncs_keep_alive() {
    let _ = atol;
    let _ = strtoi64;
    let _ = strtou64;
}
