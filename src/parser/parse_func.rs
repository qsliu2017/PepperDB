#![allow(unreachable_patterns)]
/*-------------------------------------------------------------------------
 *
 * parse_func.rs
 *      handle function calls in parser
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/backend/parser/parse_func.c
 *
 *-------------------------------------------------------------------------
 */

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void};
use core::mem::size_of;

use crate::{castNode, current_cell, foreach, foreach_delete_current, lfirst_node, list_make2, makeNode, strVal, IsA};
use crate::c::{OidIsValid, int32};
use crate::postgres_ext::{Oid, InvalidOid};
use crate::postgres::{Datum, ObjectIdGetDatum};

use crate::nodes::nodes::{nodeTag, Node, NodeTag, NodeTag::*};
use crate::nodes::pg_list::{
    List, ListCell,
    lappend, linitial, llast,
    list_head, list_length, lnext,
    list_nth_cell, list_truncate, list_copy_tail,
    list_delete_first_n,
    NIL,
};
use crate::nodes::bitmapset::{Bitmapset, bms_add_member, bms_is_member, bms_free};
use crate::nodes::nodeFuncs::{exprType, exprLocation};
use crate::nodes::makefuncs::{makeTypeNameFromNameList};
use crate::nodes::primnodes::{
    Expr, Param, NamedArgExpr, Var, Const, ArrayExpr,
    FuncExpr, Aggref, WindowFunc, FieldSelect,
    CoercionForm, CoercionForm::*, CoercionContext, CoercionContext::*,
};
use crate::nodes::parsenodes::{
    FuncCall, WindowDef, TypeName, ObjectWithArgs, FunctionParameter,
    ObjectType, ObjectType::*,
};
use crate::parser::parse_node::ParseExprKind::{self, *};
use crate::catalog::pg_aggregate::{
    FormData_pg_aggregate, Form_pg_aggregate,
    AGGKIND_NORMAL, AGGKIND_HYPOTHETICAL, AGGKIND_IS_ORDERED_SET,
};
use crate::catalog::pg_proc::{Form_pg_proc, PROKIND_FUNCTION, PROKIND_PROCEDURE, PROKIND_AGGREGATE, PROKIND_WINDOW};
use crate::catalog::pg_type_d::{VOIDOID, ANYOID, RECORDOID, UNKNOWNOID};

use crate::access::htup_details::{HeapTuple, HeapTupleIsValid, GETSTRUCT};
use crate::access::attnum::InvalidAttrNumber;
use crate::access::common::tupdesc::TupleDescAttr;

use crate::utils::cache::syscache::{
    SearchSysCache1, ReleaseSysCache, SysCacheGetAttrNotNull,
};
use crate::utils::cache::lsyscache::{
    get_array_type, get_base_element_type, getBaseType,
    get_type_category_preferred, get_func_prokind,
    TextDatumGetCString,
};
use crate::utils::builtins::format_type_be;
use crate::utils::mmgr::mcxt::pfree;

use crate::catalog::namespace::{
    FuncnameGetCandidates, FuncCandidateList, _FuncCandidateList,
    NameListToString,
};
use crate::nodes::read::stringToNode;

use crate::lib::stringinfo::{
    StringInfoData, StringInfo,
    initStringInfo, appendStringInfoString, appendStringInfoChar,
};
use crate::appendStringInfo;

use crate::parser::parse_node::{
    ParseState, ParseCallbackState,
    setup_parser_errposition_callback, cancel_parser_errposition_callback,
    parser_errposition,
};
use crate::parser::parse_coerce::{
    coerce_type, can_coerce_type,
    enforce_generic_type_consistency,
    select_common_type, select_common_typmod, coerce_to_common_type,
    find_coercion_pathway, CoercionPathType, CoercionPathType::*,
    TypeCategory, IsPreferredType, TYPCATEGORY,
};
use crate::catalog::pg_type::{TYPCATEGORY_INVALID, TYPCATEGORY_STRING};

/* FuncDetailCode type and base FUNCDETAIL_* constants (defined here; AGGREGATE/etc below) */
pub type FuncDetailCode = c_int;
pub const FUNCDETAIL_NOTFOUND:  FuncDetailCode = 0;
pub const FUNCDETAIL_MULTIPLE:  FuncDetailCode = 1;
pub const FUNCDETAIL_NORMAL:    FuncDetailCode = 2;
use crate::parser::parse_expr::transformExpr;
use crate::parser::parse_clause::transformWhereClause;
use crate::parser::parse_relation::{
    GetNSItemByRangeTablePosn, scanNSItemForColumn,
};
use crate::parser::parse_type::{
    LookupTypeNameExtended, LookupTypeNameOid,
    typeTypeId, typeTypeRelid,
};
use crate::ISCOMPLEX;
use crate::pg_config_manual::FUNC_MAX_ARGS;

/* FUNCDETAIL constants not in parse_oper -- defined here for parse_func's use */
pub const FUNCDETAIL_AGGREGATE: FuncDetailCode = 3;
pub const FUNCDETAIL_COERCION:  FuncDetailCode = 4;
pub const FUNCDETAIL_WINDOWFUNC: FuncDetailCode = 5;
pub const FUNCDETAIL_PROCEDURE: FuncDetailCode = 6;

/* Syscache IDs used locally (mirror parse_coerce.rs convention) */
const PROCOID:  c_int = 0; // TODO(pg-port): real value from syscache_ids.h
const AGGFNOID: c_int = 0; // TODO(pg-port): real value from syscache_ids.h

/* Anum for pg_proc.proargdefaults (column 20 per pg_proc.h) */
const Anum_pg_proc_proargdefaults: i16 = 20; // TODO(pg-port): verify against pg_proc_d.h

/* Possible error codes from LookupFuncNameInternal */
#[derive(PartialEq)]
#[repr(C)]
enum FuncLookupError {
    FUNCLOOKUP_NOSUCHFUNC,
    FUNCLOOKUP_AMBIGUOUS,
}
use FuncLookupError::*;

/* forward declarations for static helpers defined later in this file */

/*
 * Parse a function call
 *
 * For historical reasons, Postgres tries to treat the notations tab.col
 * and col(tab) as equivalent: if a single-argument function call has an
 * argument of complex type and the (unqualified) function name matches
 * any attribute of the type, we can interpret it as a column projection.
 * Conversely a function of a single complex-type argument can be written
 * like a column reference, allowing functions to act like computed columns.
 *
 * If both interpretations are possible, we prefer the one matching the
 * syntactic form, but otherwise the form does not matter.
 *
 * Hence, both cases come through here.  If fn is null, we're dealing with
 * column syntax not function syntax.  In the function-syntax case,
 * the FuncCall struct is needed to carry various decoration that applies
 * to aggregate and window functions.
 *
 * Also, when fn is null, we return NULL on failure rather than
 * reporting a no-such-function error.
 *
 * The argument expressions (in fargs) must have been transformed
 * already.  However, nothing in *fn has been transformed.
 *
 * last_srf should be a copy of pstate->p_last_srf from just before we
 * started transforming fargs.  If the caller knows that fargs couldn't
 * contain any SRF calls, last_srf can just be pstate->p_last_srf.
 *
 * proc_call is true if we are considering a CALL statement, so that the
 * name must resolve to a procedure name, not anything else.  This flag
 * also specifies that the argument list includes any OUT-mode arguments.
 */
pub unsafe fn ParseFuncOrColumn(
    pstate: *mut ParseState,
    funcname: *mut List,
    mut fargs: *mut List,
    last_srf: *mut Node,
    fn_: *mut FuncCall,
    proc_call: bool,
    location: c_int,
) -> *mut Node {
    let is_column = fn_.is_null();
    let agg_order: *mut List = if !fn_.is_null() { (*fn_).agg_order } else { NIL };
    let mut agg_filter: *mut Expr = core::ptr::null_mut();
    let over: *mut WindowDef = if !fn_.is_null() { (*fn_).over } else { core::ptr::null_mut() };
    let agg_within_group: bool = if !fn_.is_null() { (*fn_).agg_within_group } else { false };
    let agg_star: bool = if !fn_.is_null() { (*fn_).agg_star } else { false };
    let agg_distinct: bool = if !fn_.is_null() { (*fn_).agg_distinct } else { false };
    let mut func_variadic: bool = if !fn_.is_null() { (*fn_).func_variadic } else { false };
    let funcformat: CoercionForm = if !fn_.is_null() { (*fn_).funcformat } else { COERCE_EXPLICIT_CALL };
    let could_be_projection: bool;
    let mut rettype: Oid = InvalidOid;
    let mut funcid: Oid = InvalidOid;
    let mut first_arg: *mut Node = core::ptr::null_mut();
    let mut nargs: c_int = 0;
    let mut nargsplusdefs: c_int;
    let mut actual_arg_types: [Oid; FUNC_MAX_ARGS as usize] = [0; FUNC_MAX_ARGS as usize];
    let mut declared_arg_types: *mut Oid = core::ptr::null_mut();
    let mut argnames: *mut List = NIL;
    let mut argdefaults: *mut List = NIL;
    let retval: *mut Node;
    let mut retset: bool = false;
    let mut nvargs: c_int = 0;
    let mut vatype: Oid = InvalidOid;
    let fdresult: FuncDetailCode;
    let mut aggkind: c_char = 0;
    let mut pcbstate = core::mem::zeroed::<ParseCallbackState>();

    /*
     * If there's an aggregate filter, transform it using transformWhereClause
     */
    if !fn_.is_null() && !(*fn_).agg_filter.is_null() {
        agg_filter = transformWhereClause(pstate, (*fn_).agg_filter,
                                          EXPR_KIND_FILTER,
                                          c"FILTER".as_ptr()) as *mut Expr;
    }

    /*
     * Most of the rest of the parser just assumes that functions do not have
     * more than FUNC_MAX_ARGS parameters.  We have to test here to protect
     * against array overruns, etc.  Of course, this may not be a function,
     * but the test doesn't hurt.
     */
    if list_length(fargs) > FUNC_MAX_ARGS as c_int {
        ereport!(ERROR,
            errmsg!("cannot pass more than {} arguments to a function",
                    FUNC_MAX_ARGS)
            /* C also: errcode(ERRCODE_TOO_MANY_ARGUMENTS), parser_errposition */
        );
    }

    /*
     * Extract arg type info in preparation for function lookup.
     *
     * If any arguments are Param markers of type VOID, we discard them from
     * the parameter list. This is a hack to allow the JDBC driver to not have
     * to distinguish "input" and "output" parameter symbols while parsing
     * function-call constructs.  Don't do this if dealing with column syntax,
     * nor if we had WITHIN GROUP (because in that case it's critical to keep
     * the argument count unchanged).
     */
    foreach!(l, fargs, {
        let lc = current_cell!(l);
        let arg = *(lc as *mut *mut Node); // lfirst(l)
        let argtype = exprType(arg);
        if argtype == VOIDOID && IsA!(arg, T_Param) && !is_column && !agg_within_group {
            fargs = foreach_delete_current!(fargs, l);
            continue;
        }
        actual_arg_types[nargs as usize] = argtype;
        nargs += 1;
    });

    /*
     * Check for named arguments; if there are any, build a list of names.
     *
     * We allow mixed notation (some named and some not), but only with all
     * the named parameters after all the unnamed ones.  So the name list
     * corresponds to the last N actual parameters and we don't need any extra
     * bookkeeping to match things up.
     */
    argnames = NIL;
    {
        let mut l: *mut ListCell = list_head(fargs);
        while !l.is_null() {
            let arg = *(l as *mut *mut Node);
            if IsA!(arg, T_NamedArgExpr) {
                let na = arg as *mut NamedArgExpr;
                /* Reject duplicate arg names */
                let mut lc: *mut ListCell = list_head(argnames);
                while !lc.is_null() {
                    if libc_strcmp((*na).name, *(lc as *mut *mut c_char)) == 0 {
                        ereport!(ERROR,
                            errmsg!("argument name \"{}\" used more than once",
                                    cstr_to_str((*na).name))
                            /* C also: errcode(ERRCODE_SYNTAX_ERROR), parser_errposition */
                        );
                    }
                    lc = lnext(argnames, lc);
                }
                argnames = lappend(argnames, (*na).name as *mut c_void);
            } else {
                if !argnames.is_null() && list_length(argnames) > 0 {
                    ereport!(ERROR,
                        errmsg!("positional argument cannot follow named argument")
                        /* C also: errcode(ERRCODE_SYNTAX_ERROR), parser_errposition */
                    );
                }
            }
            l = lnext(fargs, l);
        }
    }

    if !fargs.is_null() {
        first_arg = linitial(fargs) as *mut Node;
        debug_assert!(!first_arg.is_null());
    }

    /*
     * Decide whether it's legitimate to consider the construct to be a column
     * projection.  For that, there has to be a single argument of complex
     * type, the function name must not be qualified, and there cannot be any
     * syntactic decoration that'd require it to be a function (such as
     * aggregate or variadic decoration, or named arguments).
     */
    could_be_projection = nargs == 1
        && !proc_call
        && (agg_order.is_null() || list_length(agg_order) == 0)
        && agg_filter.is_null()
        && !agg_star
        && !agg_distinct
        && over.is_null()
        && !func_variadic
        && (argnames.is_null() || list_length(argnames) == 0)
        && list_length(funcname) == 1
        && (actual_arg_types[0] == RECORDOID || ISCOMPLEX!(actual_arg_types[0]));

    /*
     * If it's column syntax, check for column projection case first.
     */
    if could_be_projection && is_column {
        let rv = ParseComplexProjection(pstate,
                                        strVal!(linitial(funcname) as *mut Node),
                                        first_arg,
                                        location);
        if !rv.is_null() {
            return rv;
        }
        /*
         * If ParseComplexProjection doesn't recognize it as a projection,
         * just press on.
         */
    }

    /*
     * func_get_detail looks up the function in the catalogs, does
     * disambiguation for polymorphic functions, handles inheritance, and
     * returns the funcid and type and set or singleton status of the
     * function's return value.  It also returns the true argument types to
     * the function.
     *
     * Note: for a named-notation or variadic function call, the reported
     * "true" types aren't really what is in pg_proc: the types are reordered
     * to match the given argument order of named arguments, and a variadic
     * argument is replaced by a suitable number of copies of its element
     * type.  We'll fix up the variadic case below.  We may also have to deal
     * with default arguments.
     */
    setup_parser_errposition_callback(&mut pcbstate, pstate, location);

    fdresult = func_get_detail(funcname, fargs, argnames, nargs,
                               actual_arg_types.as_mut_ptr(),
                               !func_variadic, true, proc_call,
                               &mut funcid, &mut rettype, &mut retset,
                               &mut nvargs, &mut vatype,
                               &mut declared_arg_types, &mut argdefaults);

    cancel_parser_errposition_callback(&mut pcbstate);

    /*
     * Check for various wrong-kind-of-routine cases.
     */

    /* If this is a CALL, reject things that aren't procedures */
    if proc_call
        && (fdresult == FUNCDETAIL_NORMAL
            || fdresult == FUNCDETAIL_AGGREGATE
            || fdresult == FUNCDETAIL_WINDOWFUNC
            || fdresult == FUNCDETAIL_COERCION)
    {
        ereport!(ERROR,
            errmsg!("{} is not a procedure",
                    cstr_to_str(func_signature_string(funcname, nargs, argnames,
                                                      actual_arg_types.as_ptr())))
            /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE), errhint, parser_errposition */
        );
    }
    /* Conversely, if not a CALL, reject procedures */
    if fdresult == FUNCDETAIL_PROCEDURE && !proc_call {
        ereport!(ERROR,
            errmsg!("{} is a procedure",
                    cstr_to_str(func_signature_string(funcname, nargs, argnames,
                                                      actual_arg_types.as_ptr())))
            /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE), errhint("To call a procedure, use CALL."), parser_errposition */
        );
    }

    if fdresult == FUNCDETAIL_NORMAL
        || fdresult == FUNCDETAIL_PROCEDURE
        || fdresult == FUNCDETAIL_COERCION
    {
        /*
         * In these cases, complain if there was anything indicating it must
         * be an aggregate or window function.
         */
        if agg_star {
            ereport!(ERROR,
                errmsg!("{}(*) specified, but {} is not an aggregate function",
                        cstr_to_str(NameListToString(funcname)),
                        cstr_to_str(NameListToString(funcname)))
                /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE), parser_errposition */
            );
        }
        if agg_distinct {
            ereport!(ERROR,
                errmsg!("DISTINCT specified, but {} is not an aggregate function",
                        cstr_to_str(NameListToString(funcname)))
            );
        }
        if agg_within_group {
            ereport!(ERROR,
                errmsg!("WITHIN GROUP specified, but {} is not an aggregate function",
                        cstr_to_str(NameListToString(funcname)))
            );
        }
        if !agg_order.is_null() && list_length(agg_order) > 0 {
            ereport!(ERROR,
                errmsg!("ORDER BY specified, but {} is not an aggregate function",
                        cstr_to_str(NameListToString(funcname)))
            );
        }
        if !agg_filter.is_null() {
            ereport!(ERROR,
                errmsg!("FILTER specified, but {} is not an aggregate function",
                        cstr_to_str(NameListToString(funcname)))
            );
        }
        if !over.is_null() {
            ereport!(ERROR,
                errmsg!("OVER specified, but {} is not a window function nor an aggregate function",
                        cstr_to_str(NameListToString(funcname)))
            );
        }
    }

    /*
     * So far so good, so do some fdresult-type-specific processing.
     */
    if fdresult == FUNCDETAIL_NORMAL || fdresult == FUNCDETAIL_PROCEDURE {
        /* Nothing special to do for these cases. */
    } else if fdresult == FUNCDETAIL_AGGREGATE {
        /*
         * It's an aggregate; fetch needed info from the pg_aggregate entry.
         */
        let tup: HeapTuple;
        let class_form: Form_pg_aggregate;
        let cat_direct_args: c_int;

        tup = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(funcid));
        if !HeapTupleIsValid(tup) {
            /* should not happen */
            elog!(ERROR, "cache lookup failed for aggregate {}", funcid);
        }
        class_form = GETSTRUCT(tup) as Form_pg_aggregate;
        aggkind = (*class_form).aggkind;
        cat_direct_args = (*class_form).aggnumdirectargs as c_int;
        ReleaseSysCache(tup);

        /* Now check various disallowed cases. */
        if AGGKIND_IS_ORDERED_SET(aggkind) {
            let num_aggregated_args: c_int;
            let num_direct_args: c_int;

            if !agg_within_group {
                ereport!(ERROR,
                    errmsg!("WITHIN GROUP is required for ordered-set aggregate {}",
                            cstr_to_str(NameListToString(funcname)))
                    /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE), parser_errposition */
                );
            }
            if !over.is_null() {
                ereport!(ERROR,
                    errmsg!("OVER is not supported for ordered-set aggregate {}",
                            cstr_to_str(NameListToString(funcname)))
                    /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED), parser_errposition */
                );
            }
            /* gram.y rejects DISTINCT + WITHIN GROUP */
            debug_assert!(!agg_distinct);
            /* gram.y rejects VARIADIC + WITHIN GROUP */
            debug_assert!(!func_variadic);

            /*
             * Since func_get_detail was working with an undifferentiated list
             * of arguments, it might have selected an aggregate that doesn't
             * really match because it requires a different division of direct
             * and aggregated arguments.  Check that the number of direct
             * arguments is actually OK; if not, throw an "undefined function"
             * error, similarly to the case where a misplaced ORDER BY is used
             * in a regular aggregate call.
             */
            num_aggregated_args = list_length(agg_order);
            num_direct_args = nargs - num_aggregated_args;
            debug_assert!(num_direct_args >= 0);

            if !OidIsValid(vatype) {
                /* Test is simple if aggregate isn't variadic */
                if num_direct_args != cat_direct_args {
                    ereport!(ERROR,
                        errmsg!("function {} does not exist",
                                cstr_to_str(func_signature_string(funcname, nargs, argnames,
                                                                  actual_arg_types.as_ptr())))
                        /* C also: errcode(ERRCODE_UNDEFINED_FUNCTION), errhint_plural, parser_errposition */
                    );
                }
            } else {
                /*
                 * If it's variadic, we have two cases depending on whether
                 * the agg was "... ORDER BY VARIADIC" or "..., VARIADIC ORDER
                 * BY VARIADIC".  It's the latter if catDirectArgs equals
                 * pronargs; to save a catalog lookup, we reverse-engineer
                 * pronargs from the info we got from func_get_detail.
                 */
                let pronargs: c_int;

                pronargs = if nvargs > 1 { nargs - nvargs + 1 } else { nargs };
                if cat_direct_args < pronargs {
                    /* VARIADIC isn't part of direct args, so still easy */
                    if num_direct_args != cat_direct_args {
                        ereport!(ERROR,
                            errmsg!("function {} does not exist",
                                    cstr_to_str(func_signature_string(funcname, nargs, argnames,
                                                                      actual_arg_types.as_ptr())))
                            /* C also: errcode(ERRCODE_UNDEFINED_FUNCTION), errhint_plural, parser_errposition */
                        );
                    }
                } else {
                    /*
                     * Both direct and aggregated args were declared variadic.
                     * For a standard ordered-set aggregate, it's okay as long
                     * as there aren't too few direct args.  For a
                     * hypothetical-set aggregate, we assume that the
                     * hypothetical arguments are those that matched the
                     * variadic parameter; there must be just as many of them
                     * as there are aggregated arguments.
                     */
                    if aggkind == AGGKIND_HYPOTHETICAL {
                        if nvargs != 2 * num_aggregated_args {
                            ereport!(ERROR,
                                errmsg!("function {} does not exist",
                                        cstr_to_str(func_signature_string(funcname, nargs, argnames,
                                                                          actual_arg_types.as_ptr())))
                                /* C also: errcode(ERRCODE_UNDEFINED_FUNCTION), errhint, parser_errposition */
                            );
                        }
                    } else {
                        if nvargs <= num_aggregated_args {
                            ereport!(ERROR,
                                errmsg!("function {} does not exist",
                                        cstr_to_str(func_signature_string(funcname, nargs, argnames,
                                                                          actual_arg_types.as_ptr())))
                                /* C also: errcode(ERRCODE_UNDEFINED_FUNCTION), errhint_plural, parser_errposition */
                            );
                        }
                    }
                }
            }

            /* Check type matching of hypothetical arguments */
            if aggkind == AGGKIND_HYPOTHETICAL {
                unify_hypothetical_args(pstate, fargs, num_aggregated_args,
                                        actual_arg_types.as_mut_ptr(), declared_arg_types);
            }
        } else {
            /* Normal aggregate, so it can't have WITHIN GROUP */
            if agg_within_group {
                ereport!(ERROR,
                    errmsg!("{} is not an ordered-set aggregate, so it cannot have WITHIN GROUP",
                            cstr_to_str(NameListToString(funcname)))
                    /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE), parser_errposition */
                );
            }
        }
    } else if fdresult == FUNCDETAIL_WINDOWFUNC {
        /*
         * True window functions must be called with a window definition.
         */
        if over.is_null() {
            ereport!(ERROR,
                errmsg!("window function {} requires an OVER clause",
                        cstr_to_str(NameListToString(funcname)))
                /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE), parser_errposition */
            );
        }
        /* And, per spec, WITHIN GROUP isn't allowed */
        if agg_within_group {
            ereport!(ERROR,
                errmsg!("window function {} cannot have WITHIN GROUP",
                        cstr_to_str(NameListToString(funcname)))
                /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE), parser_errposition */
            );
        }
    } else if fdresult == FUNCDETAIL_COERCION {
        /*
         * We interpreted it as a type coercion. coerce_type can handle these
         * cases, so why duplicate code...
         */
        return coerce_type(pstate, linitial(fargs) as *mut Node,
                           actual_arg_types[0], rettype, -1,
                           COERCION_EXPLICIT, COERCE_EXPLICIT_CALL, location);
    } else if fdresult == FUNCDETAIL_MULTIPLE {
        /*
         * We found multiple possible functional matches.  If we are dealing
         * with attribute notation, return failure, letting the caller report
         * "no such column" (we already determined there wasn't one).  If
         * dealing with function notation, report "ambiguous function",
         * regardless of whether there's also a column by this name.
         */
        if is_column {
            return core::ptr::null_mut();
        }

        if proc_call {
            ereport!(ERROR,
                errmsg!("procedure {} is not unique",
                        cstr_to_str(func_signature_string(funcname, nargs, argnames,
                                                          actual_arg_types.as_ptr())))
                /* C also: errcode(ERRCODE_AMBIGUOUS_FUNCTION), errhint, parser_errposition */
            );
        } else {
            ereport!(ERROR,
                errmsg!("function {} is not unique",
                        cstr_to_str(func_signature_string(funcname, nargs, argnames,
                                                          actual_arg_types.as_ptr())))
                /* C also: errcode(ERRCODE_AMBIGUOUS_FUNCTION), errhint, parser_errposition */
            );
        }
    } else {
        /*
         * Not found as a function.  If we are dealing with attribute
         * notation, return failure, letting the caller report "no such
         * column" (we already determined there wasn't one).
         */
        if is_column {
            return core::ptr::null_mut();
        }

        /*
         * Check for column projection interpretation, since we didn't before.
         */
        if could_be_projection {
            let rv = ParseComplexProjection(pstate,
                                            strVal!(linitial(funcname) as *mut Node),
                                            first_arg,
                                            location);
            if !rv.is_null() {
                return rv;
            }
        }

        /*
         * No function, and no column either.  Since we're dealing with
         * function notation, report "function does not exist".
         */
        if !agg_order.is_null() && list_length(agg_order) > 1 && !agg_within_group {
            /* It's agg(x, ORDER BY y,z) ... perhaps misplaced ORDER BY */
            ereport!(ERROR,
                errmsg!("function {} does not exist",
                        cstr_to_str(func_signature_string(funcname, nargs, argnames,
                                                          actual_arg_types.as_ptr())))
                /* C also: errcode(ERRCODE_UNDEFINED_FUNCTION), errhint (misplaced ORDER BY), parser_errposition */
            );
        } else if proc_call {
            ereport!(ERROR,
                errmsg!("procedure {} does not exist",
                        cstr_to_str(func_signature_string(funcname, nargs, argnames,
                                                          actual_arg_types.as_ptr())))
                /* C also: errcode(ERRCODE_UNDEFINED_FUNCTION), errhint, parser_errposition */
            );
        } else {
            ereport!(ERROR,
                errmsg!("function {} does not exist",
                        cstr_to_str(func_signature_string(funcname, nargs, argnames,
                                                          actual_arg_types.as_ptr())))
                /* C also: errcode(ERRCODE_UNDEFINED_FUNCTION), errhint, parser_errposition */
            );
        }
    }

    /*
     * If there are default arguments, we have to include their types in
     * actual_arg_types for the purpose of checking generic type consistency.
     * However, we do NOT put them into the generated parse node, because
     * their actual values might change before the query gets run.  The
     * planner has to insert the up-to-date values at plan time.
     */
    nargsplusdefs = nargs;
    {
        let mut l: *mut ListCell = list_head(argdefaults);
        while !l.is_null() {
            let expr = *(l as *mut *mut Node);
            /* probably shouldn't happen ... */
            if nargsplusdefs >= FUNC_MAX_ARGS as c_int {
                ereport!(ERROR,
                    errmsg!("cannot pass more than {} arguments to a function",
                            FUNC_MAX_ARGS)
                    /* C also: errcode(ERRCODE_TOO_MANY_ARGUMENTS), parser_errposition */
                );
            }
            actual_arg_types[nargsplusdefs as usize] = exprType(expr);
            nargsplusdefs += 1;
            l = lnext(argdefaults, l);
        }
    }

    /*
     * enforce consistency with polymorphic argument and return types,
     * possibly adjusting return type or declared_arg_types (which will be
     * used as the cast destination by make_fn_arguments)
     */
    rettype = enforce_generic_type_consistency(actual_arg_types.as_mut_ptr(),
                                               declared_arg_types,
                                               nargsplusdefs,
                                               rettype,
                                               false);

    /* perform the necessary typecasting of arguments */
    make_fn_arguments(pstate, fargs, actual_arg_types.as_mut_ptr(), declared_arg_types);

    /*
     * If the function isn't actually variadic, forget any VARIADIC decoration
     * on the call.  (Perhaps we should throw an error instead, but
     * historically we've allowed people to write that.)
     */
    if !OidIsValid(vatype) {
        debug_assert!(nvargs == 0);
        func_variadic = false;
    }

    /*
     * If it's a variadic function call, transform the last nvargs arguments
     * into an array --- unless it's an "any" variadic.
     */
    if nvargs > 0 && vatype != ANYOID {
        let newa: *mut ArrayExpr = makeNode!(ArrayExpr, T_ArrayExpr);
        let non_var_args: c_int = nargs - nvargs;
        let vargs: *mut List;

        debug_assert!(non_var_args >= 0);
        vargs = list_copy_tail(fargs, non_var_args);
        fargs = list_truncate(fargs, non_var_args);

        (*newa).elements = vargs;
        /* assume all the variadic arguments were coerced to the same type */
        (*newa).element_typeid = exprType(linitial(vargs) as *mut Node);
        (*newa).array_typeid = get_array_type((*newa).element_typeid);
        if !OidIsValid((*newa).array_typeid) {
            ereport!(ERROR,
                errmsg!("could not find array type for data type {}",
                        cstr_to_str(format_type_be((*newa).element_typeid)))
                /* C also: errcode(ERRCODE_UNDEFINED_OBJECT), parser_errposition */
            );
        }
        /* array_collid will be set by parse_collate.c */
        (*newa).multidims = false;
        (*newa).location = exprLocation(linitial(vargs) as *const Node);

        fargs = lappend(fargs, newa as *mut c_void);

        /* We could not have had VARIADIC marking before ... */
        debug_assert!(!func_variadic);
        /* ... but now, it's a VARIADIC call */
        func_variadic = true;
    }

    /*
     * If an "any" variadic is called with explicit VARIADIC marking, insist
     * that the variadic parameter be of some array type.
     */
    if nargs > 0 && vatype == ANYOID && func_variadic {
        let va_arr_typid: Oid = actual_arg_types[(nargs - 1) as usize];
        if !OidIsValid(get_base_element_type(va_arr_typid)) {
            ereport!(ERROR,
                errmsg!("VARIADIC argument must be an array")
                /* C also: errcode(ERRCODE_DATATYPE_MISMATCH), parser_errposition */
            );
        }
    }

    /* if it returns a set, check that's OK */
    if retset {
        check_srf_call_placement(pstate, last_srf, location);
    }

    /* build the appropriate output structure */
    if fdresult == FUNCDETAIL_NORMAL || fdresult == FUNCDETAIL_PROCEDURE {
        let funcexpr: *mut FuncExpr = makeNode!(FuncExpr, T_FuncExpr);

        (*funcexpr).funcid = funcid;
        (*funcexpr).funcresulttype = rettype;
        (*funcexpr).funcretset = retset;
        (*funcexpr).funcvariadic = func_variadic;
        (*funcexpr).funcformat = funcformat;
        /* funccollid and inputcollid will be set by parse_collate.c */
        (*funcexpr).args = fargs;
        (*funcexpr).location = location;

        retval = funcexpr as *mut Node;
    } else if fdresult == FUNCDETAIL_AGGREGATE && over.is_null() {
        /* aggregate function */
        let aggref: *mut Aggref = makeNode!(Aggref, T_Aggref);

        (*aggref).aggfnoid = funcid;
        (*aggref).aggtype = rettype;
        /* aggcollid and inputcollid will be set by parse_collate.c */
        (*aggref).aggtranstype = InvalidOid; /* will be set by planner */
        /* aggargtypes will be set by transformAggregateCall */
        /* aggdirectargs and args will be set by transformAggregateCall */
        /* aggorder and aggdistinct will be set by transformAggregateCall */
        (*aggref).aggfilter = agg_filter;
        (*aggref).aggstar = agg_star;
        (*aggref).aggvariadic = func_variadic;
        (*aggref).aggkind = aggkind;
        (*aggref).aggpresorted = false;
        /* agglevelsup will be set by transformAggregateCall */
        (*aggref).aggsplit = crate::nodes::nodes::AggSplit::AGGSPLIT_SIMPLE; /* planner might change this */
        (*aggref).aggno = -1; /* planner will set aggno and aggtransno */
        (*aggref).aggtransno = -1;
        (*aggref).location = location;

        /*
         * Reject attempt to call a parameterless aggregate without (*)
         * syntax.  This is mere pedantry but some folks insisted ...
         */
        if fargs.is_null() && !agg_star && !agg_within_group {
            ereport!(ERROR,
                errmsg!("{}(*) must be used to call a parameterless aggregate function",
                        cstr_to_str(NameListToString(funcname)))
                /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE), parser_errposition */
            );
        }

        if retset {
            ereport!(ERROR,
                errmsg!("aggregates cannot return sets")
                /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION), parser_errposition */
            );
        }

        /*
         * We might want to support named arguments later, but disallow it for
         * now.  We'd need to figure out the parsed representation (should the
         * NamedArgExprs go above or below the TargetEntry nodes?) and then
         * teach the planner to reorder the list properly.  Or maybe we could
         * make transformAggregateCall do that?  However, if you'd also like
         * to allow default arguments for aggregates, we'd need to do it in
         * planning to avoid semantic problems.
         */
        if !argnames.is_null() && list_length(argnames) > 0 {
            ereport!(ERROR,
                errmsg!("aggregates cannot use named arguments")
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED), parser_errposition */
            );
        }

        /* parse_agg.c does additional aggregate-specific processing */
        /* TODO(pg-port): transformAggregateCall not yet ported */
        // transformAggregateCall(pstate, aggref, fargs, agg_order, agg_distinct);

        retval = aggref as *mut Node;
    } else {
        /* window function */
        let wfunc: *mut WindowFunc = makeNode!(WindowFunc, T_WindowFunc);

        debug_assert!(!over.is_null()); /* lack of this was checked above */
        debug_assert!(!agg_within_group); /* also checked above */

        (*wfunc).winfnoid = funcid;
        (*wfunc).wintype = rettype;
        /* wincollid and inputcollid will be set by parse_collate.c */
        (*wfunc).args = fargs;
        /* winref will be set by transformWindowFuncCall */
        (*wfunc).winstar = agg_star;
        (*wfunc).winagg = fdresult == FUNCDETAIL_AGGREGATE;
        (*wfunc).aggfilter = agg_filter;
        (*wfunc).runCondition = NIL;
        (*wfunc).location = location;

        /*
         * agg_star is allowed for aggregate functions but distinct isn't
         */
        if agg_distinct {
            ereport!(ERROR,
                errmsg!("DISTINCT is not implemented for window functions")
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED), parser_errposition */
            );
        }

        /*
         * Reject attempt to call a parameterless aggregate without (*)
         * syntax.  This is mere pedantry but some folks insisted ...
         */
        if (*wfunc).winagg && fargs.is_null() && !agg_star {
            ereport!(ERROR,
                errmsg!("{}(*) must be used to call a parameterless aggregate function",
                        cstr_to_str(NameListToString(funcname)))
                /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE), parser_errposition */
            );
        }

        /*
         * ordered aggs not allowed in windows yet
         */
        if !agg_order.is_null() && list_length(agg_order) > 0 {
            ereport!(ERROR,
                errmsg!("aggregate ORDER BY is not implemented for window functions")
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED), parser_errposition */
            );
        }

        /*
         * FILTER is not yet supported with true window functions
         */
        if !(*wfunc).winagg && !agg_filter.is_null() {
            ereport!(ERROR,
                errmsg!("FILTER is not implemented for non-aggregate window functions")
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED), parser_errposition */
            );
        }

        /*
         * Window functions can't either take or return sets
         */
        if (*pstate).p_last_srf != last_srf {
            ereport!(ERROR,
                errmsg!("window function calls cannot contain set-returning function calls")
                /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errhint, parser_errposition */
            );
        }

        if retset {
            ereport!(ERROR,
                errmsg!("window functions cannot return sets")
                /* C also: errcode(ERRCODE_INVALID_FUNCTION_DEFINITION), parser_errposition */
            );
        }

        /* parse_agg.c does additional window-func-specific processing */
        /* TODO(pg-port): transformWindowFuncCall not yet ported */
        // transformWindowFuncCall(pstate, wfunc, over);

        retval = wfunc as *mut Node;
    }

    /* if it returns a set, remember it for error checks at higher levels */
    if retset {
        (*pstate).p_last_srf = retval;
    }

    retval
}

/* func_match_argtypes()
 *
 * Given a list of candidate functions (having the right name and number
 * of arguments) and an array of input datatype OIDs, produce a shortlist of
 * those candidates that actually accept the input datatypes (either exactly
 * or by coercion), and return the number of such candidates.
 *
 * Note that can_coerce_type will assume that UNKNOWN inputs are coercible to
 * anything, so candidates will not be eliminated on that basis.
 *
 * NB: okay to modify input list structure, as long as we find at least
 * one match.  If no match at all, the list must remain unmodified.
 */
pub unsafe fn func_match_argtypes(
    nargs: c_int,
    input_typeids: *mut Oid,
    raw_candidates: FuncCandidateList,
    candidates: *mut FuncCandidateList, /* return value */
) -> c_int {
    let mut current_candidate: FuncCandidateList;
    let mut next_candidate: FuncCandidateList;
    let mut ncandidates: c_int = 0;

    *candidates = core::ptr::null_mut();

    current_candidate = raw_candidates;
    while !current_candidate.is_null() {
        next_candidate = (*current_candidate).next;
        if can_coerce_type(nargs, input_typeids, (*current_candidate).args.as_mut_ptr(),
                           COERCION_IMPLICIT) {
            (*current_candidate).next = *candidates;
            *candidates = current_candidate;
            ncandidates += 1;
        }
        current_candidate = next_candidate;
    }

    ncandidates
} /* func_match_argtypes() */


/* func_select_candidate()
 *      Given the input argtype array and more than one candidate
 *      for the function, attempt to resolve the conflict.
 *
 * Returns the selected candidate if the conflict can be resolved,
 * otherwise returns NULL.
 *
 * Note that the caller has already determined that there is no candidate
 * exactly matching the input argtypes, and has pruned away any "candidates"
 * that aren't actually coercion-compatible with the input types.
 *
 * This is also used for resolving ambiguous operator references.  Formerly
 * parse_oper.c had its own, essentially duplicate code for the purpose.
 * The following comments (formerly in parse_oper.c) are kept to record some
 * of the history of these heuristics.
 *
 * OLD COMMENTS:
 *
 * This routine is new code, replacing binary_oper_select_candidate()
 * which dates from v4.2/v1.0.x days. It tries very hard to match up
 * operators with types, including allowing type coercions if necessary.
 * The important thing is that the code do as much as possible,
 * while _never_ doing the wrong thing, where "the wrong thing" would
 * be returning an operator when other better choices are available,
 * or returning an operator which is a non-intuitive possibility.
 * - thomas 1998-05-21
 *
 * The comments below came from binary_oper_select_candidate(), and
 * illustrate the issues and choices which are possible:
 * - thomas 1998-05-20
 *
 * current wisdom holds that the default operator should be one in which
 * both operands have the same type (there will only be one such
 * operator)
 *
 * 7.27.93 - I have decided not to do this; it's too hard to justify, and
 * it's easy enough to typecast explicitly - avi
 * [the rest of this routine was commented out since then - ay]
 *
 * 6/23/95 - I don't complete agree with avi. In particular, casting
 * floats is a pain for users. Whatever the rationale behind not doing
 * this is, I need the following special case to work.
 *
 * In the WHERE clause of a query, if a float is specified without
 * quotes, we treat it as float8. I added the float48* operators so
 * that we can operate on float4 and float8. But now we have more than
 * one matching operator if the right arg is unknown (eg. float
 * specified with quotes). This break some stuff in the regression
 * test where there are floats in quotes not properly casted. Below is
 * the solution. In addition to requiring the operator operates on the
 * same type for both operands [as in the code Avi originally
 * commented out], we also require that the operators be equivalent in
 * some sense. (see equivalentOpersAfterPromotion for details.)
 * - ay 6/95
 */
pub unsafe fn func_select_candidate(
    nargs: c_int,
    input_typeids: *mut Oid,
    mut candidates: FuncCandidateList,
) -> FuncCandidateList {
    let mut current_candidate: FuncCandidateList;
    let mut first_candidate: FuncCandidateList;
    let mut last_candidate: FuncCandidateList;
    let mut current_typeids: *mut Oid;
    let mut current_type: Oid;
    let mut i: c_int;
    let mut ncandidates: c_int;
    let mut nbestMatch: c_int;
    let mut nmatch: c_int;
    let mut nunknowns: c_int;
    let mut input_base_typeids: [Oid; FUNC_MAX_ARGS as usize] = [0; FUNC_MAX_ARGS as usize];
    let mut slot_category: [TYPCATEGORY; FUNC_MAX_ARGS as usize] = [TYPCATEGORY_INVALID; FUNC_MAX_ARGS as usize];
    let mut current_category: TYPCATEGORY = TYPCATEGORY_INVALID;
    let mut current_is_preferred: bool = false;
    let mut slot_has_preferred_type: [bool; FUNC_MAX_ARGS as usize] = [false; FUNC_MAX_ARGS as usize];
    let mut resolved_unknowns: bool;

    /* protect local fixed-size arrays */
    if nargs > FUNC_MAX_ARGS as c_int {
        ereport!(ERROR,
            errmsg!("cannot pass more than {} arguments to a function",
                    FUNC_MAX_ARGS)
            /* C also: errcode(ERRCODE_TOO_MANY_ARGUMENTS) */
        );
    }

    /*
     * If any input types are domains, reduce them to their base types. This
     * ensures that we will consider functions on the base type to be "exact
     * matches" in the exact-match heuristic; it also makes it possible to do
     * something useful with the type-category heuristics. Note that this
     * makes it difficult, but not impossible, to use functions declared to
     * take a domain as an input datatype.  Such a function will be selected
     * over the base-type function only if it is an exact match at all
     * argument positions, and so was already chosen by our caller.
     *
     * While we're at it, count the number of unknown-type arguments for use
     * later.
     */
    nunknowns = 0;
    i = 0;
    while i < nargs {
        if *input_typeids.add(i as usize) != UNKNOWNOID {
            input_base_typeids[i as usize] = getBaseType(*input_typeids.add(i as usize));
        } else {
            /* no need to call getBaseType on UNKNOWNOID */
            input_base_typeids[i as usize] = UNKNOWNOID;
            nunknowns += 1;
        }
        i += 1;
    }

    /*
     * Run through all candidates and keep those with the most matches on
     * exact types. Keep all candidates if none match.
     */
    ncandidates = 0;
    nbestMatch = 0;
    last_candidate = core::ptr::null_mut();
    current_candidate = candidates;
    while !current_candidate.is_null() {
        current_typeids = (*current_candidate).args.as_mut_ptr();
        nmatch = 0;
        i = 0;
        while i < nargs {
            if input_base_typeids[i as usize] != UNKNOWNOID
                && *current_typeids.add(i as usize) == input_base_typeids[i as usize]
            {
                nmatch += 1;
            }
            i += 1;
        }

        /* take this one as the best choice so far? */
        if nmatch > nbestMatch || last_candidate.is_null() {
            nbestMatch = nmatch;
            candidates = current_candidate;
            last_candidate = current_candidate;
            ncandidates = 1;
        }
        /* no worse than the last choice, so keep this one too? */
        else if nmatch == nbestMatch {
            (*last_candidate).next = current_candidate;
            last_candidate = current_candidate;
            ncandidates += 1;
        }
        /* otherwise, don't bother keeping this one... */
        current_candidate = (*current_candidate).next;
    }

    if !last_candidate.is_null() { /* terminate rebuilt list */
        (*last_candidate).next = core::ptr::null_mut();
    }

    if ncandidates == 1 {
        return candidates;
    }

    /*
     * Still too many candidates? Now look for candidates which have either
     * exact matches or preferred types at the args that will require
     * coercion. (Restriction added in 7.4: preferred type must be of same
     * category as input type; give no preference to cross-category
     * conversions to preferred types.)  Keep all candidates if none match.
     */
    i = 0;
    while i < nargs { /* avoid multiple lookups */
        slot_category[i as usize] = TypeCategory(input_base_typeids[i as usize]);
        i += 1;
    }
    ncandidates = 0;
    nbestMatch = 0;
    last_candidate = core::ptr::null_mut();
    current_candidate = candidates;
    while !current_candidate.is_null() {
        current_typeids = (*current_candidate).args.as_mut_ptr();
        nmatch = 0;
        i = 0;
        while i < nargs {
            if input_base_typeids[i as usize] != UNKNOWNOID {
                if *current_typeids.add(i as usize) == input_base_typeids[i as usize]
                    || IsPreferredType(slot_category[i as usize], *current_typeids.add(i as usize))
                {
                    nmatch += 1;
                }
            }
            i += 1;
        }

        if nmatch > nbestMatch || last_candidate.is_null() {
            nbestMatch = nmatch;
            candidates = current_candidate;
            last_candidate = current_candidate;
            ncandidates = 1;
        } else if nmatch == nbestMatch {
            (*last_candidate).next = current_candidate;
            last_candidate = current_candidate;
            ncandidates += 1;
        }
        current_candidate = (*current_candidate).next;
    }

    if !last_candidate.is_null() { /* terminate rebuilt list */
        (*last_candidate).next = core::ptr::null_mut();
    }

    if ncandidates == 1 {
        return candidates;
    }

    /*
     * Still too many candidates?  Try assigning types for the unknown inputs.
     *
     * If there are no unknown inputs, we have no more heuristics that apply,
     * and must fail.
     */
    if nunknowns == 0 {
        return core::ptr::null_mut(); /* failed to select a best candidate */
    }

    /*
     * The next step examines each unknown argument position to see if we can
     * determine a "type category" for it.  If any candidate has an input
     * datatype of STRING category, use STRING category (this bias towards
     * STRING is appropriate since unknown-type literals look like strings).
     * Otherwise, if all the candidates agree on the type category of this
     * argument position, use that category.  Otherwise, fail because we
     * cannot determine a category.
     *
     * If we are able to determine a type category, also notice whether any of
     * the candidates takes a preferred datatype within the category.
     *
     * Having completed this examination, remove candidates that accept the
     * wrong category at any unknown position.  Also, if at least one
     * candidate accepted a preferred type at a position, remove candidates
     * that accept non-preferred types.  If just one candidate remains, return
     * that one.  However, if this rule turns out to reject all candidates,
     * keep them all instead.
     */
    resolved_unknowns = false;
    i = 0;
    'outer: while i < nargs {
        let mut have_conflict: bool;

        if input_base_typeids[i as usize] != UNKNOWNOID {
            i += 1;
            continue;
        }
        resolved_unknowns = true; /* assume we can do it */
        slot_category[i as usize] = TYPCATEGORY_INVALID;
        slot_has_preferred_type[i as usize] = false;
        have_conflict = false;
        current_candidate = candidates;
        while !current_candidate.is_null() {
            current_typeids = (*current_candidate).args.as_mut_ptr();
            current_type = *current_typeids.add(i as usize);
            get_type_category_preferred(current_type,
                                        &mut current_category,
                                        &mut current_is_preferred);
            if slot_category[i as usize] == TYPCATEGORY_INVALID {
                /* first candidate */
                slot_category[i as usize] = current_category;
                slot_has_preferred_type[i as usize] = current_is_preferred;
            } else if current_category == slot_category[i as usize] {
                /* more candidates in same category */
                slot_has_preferred_type[i as usize] |= current_is_preferred;
            } else {
                /* category conflict! */
                if current_category == TYPCATEGORY_STRING {
                    /* STRING always wins if available */
                    slot_category[i as usize] = current_category;
                    slot_has_preferred_type[i as usize] = current_is_preferred;
                } else {
                    /*
                     * Remember conflict, but keep going (might find STRING)
                     */
                    have_conflict = true;
                }
            }
            current_candidate = (*current_candidate).next;
        }
        if have_conflict && slot_category[i as usize] != TYPCATEGORY_STRING {
            /* Failed to resolve category conflict at this position */
            resolved_unknowns = false;
            break 'outer;
        }
        i += 1;
    }

    if resolved_unknowns {
        /* Strip non-matching candidates */
        ncandidates = 0;
        first_candidate = candidates;
        last_candidate = core::ptr::null_mut();
        current_candidate = candidates;
        while !current_candidate.is_null() {
            let mut keepit = true;

            current_typeids = (*current_candidate).args.as_mut_ptr();
            i = 0;
            'inner: while i < nargs {
                if input_base_typeids[i as usize] != UNKNOWNOID {
                    i += 1;
                    continue 'inner;
                }
                current_type = *current_typeids.add(i as usize);
                get_type_category_preferred(current_type,
                                            &mut current_category,
                                            &mut current_is_preferred);
                if current_category != slot_category[i as usize] {
                    keepit = false;
                    break 'inner;
                }
                if slot_has_preferred_type[i as usize] && !current_is_preferred {
                    keepit = false;
                    break 'inner;
                }
                i += 1;
            }
            if keepit {
                /* keep this candidate */
                last_candidate = current_candidate;
                ncandidates += 1;
            } else {
                /* forget this candidate */
                if !last_candidate.is_null() {
                    (*last_candidate).next = (*current_candidate).next;
                } else {
                    first_candidate = (*current_candidate).next;
                }
            }
            current_candidate = (*current_candidate).next;
        }

        /* if we found any matches, restrict our attention to those */
        if !last_candidate.is_null() {
            candidates = first_candidate;
            /* terminate rebuilt list */
            (*last_candidate).next = core::ptr::null_mut();
        }

        if ncandidates == 1 {
            return candidates;
        }
    }

    /*
     * Last gasp: if there are both known- and unknown-type inputs, and all
     * the known types are the same, assume the unknown inputs are also that
     * type, and see if that gives us a unique match.  If so, use that match.
     *
     * NOTE: for a binary operator with one unknown and one non-unknown input,
     * we already tried this heuristic in binary_oper_exact().  However, that
     * code only finds exact matches, whereas here we will handle matches that
     * involve coercion, polymorphic type resolution, etc.
     */
    if nunknowns < nargs {
        let mut known_type: Oid = UNKNOWNOID;

        i = 0;
        'known: while i < nargs {
            if input_base_typeids[i as usize] == UNKNOWNOID {
                i += 1;
                continue 'known;
            }
            if known_type == UNKNOWNOID { /* first known arg? */
                known_type = input_base_typeids[i as usize];
            } else if known_type != input_base_typeids[i as usize] {
                /* oops, not all match */
                known_type = UNKNOWNOID;
                break 'known;
            }
            i += 1;
        }

        if known_type != UNKNOWNOID {
            /* okay, just one known type, apply the heuristic */
            i = 0;
            while i < nargs {
                input_base_typeids[i as usize] = known_type;
                i += 1;
            }
            ncandidates = 0;
            last_candidate = core::ptr::null_mut();
            current_candidate = candidates;
            while !current_candidate.is_null() {
                current_typeids = (*current_candidate).args.as_mut_ptr();
                if can_coerce_type(nargs, input_base_typeids.as_mut_ptr(), current_typeids,
                                   COERCION_IMPLICIT) {
                    ncandidates += 1;
                    if ncandidates > 1 {
                        break; /* not unique, give up */
                    }
                    last_candidate = current_candidate;
                }
                current_candidate = (*current_candidate).next;
            }
            if ncandidates == 1 {
                /* successfully identified a unique match */
                (*last_candidate).next = core::ptr::null_mut();
                return last_candidate;
            }
        }
    }

    core::ptr::null_mut() /* failed to select a best candidate */
} /* func_select_candidate() */

/* func_get_detail()
 *
 * Find the named function in the system catalogs.
 *
 * Attempt to find the named function in the system catalogs with
 * arguments exactly as specified, so that the normal case (exact match)
 * is as quick as possible.
 *
 * If an exact match isn't found:
 *  1) check for possible interpretation as a type coercion request
 *  2) apply the ambiguous-function resolution rules
 *
 * Return values *funcid through *true_typeids receive info about the function.
 * If argdefaults isn't NULL, *argdefaults receives a list of any default
 * argument expressions that need to be added to the given arguments.
 *
 * When processing a named- or mixed-notation call (ie, fargnames isn't NIL),
 * the returned true_typeids and argdefaults are ordered according to the
 * call's argument ordering: first any positional arguments, then the named
 * arguments, then defaulted arguments (if needed and allowed by
 * expand_defaults).  Some care is needed if this information is to be compared
 * to the function's pg_proc entry, but in practice the caller can usually
 * just work with the call's argument ordering.
 *
 * We rely primarily on fargnames/nargs/argtypes as the argument description.
 * The actual expression node list is passed in fargs so that we can check
 * for type coercion of a constant.  Some callers pass fargs == NIL indicating
 * they don't need that check made.  Note also that when fargnames isn't NIL,
 * the fargs list must be passed if the caller wants actual argument position
 * information to be returned into the NamedArgExpr nodes.
 */
pub unsafe fn func_get_detail(
    funcname: *mut List,
    fargs: *mut List,
    fargnames: *mut List,
    nargs: c_int,
    argtypes: *mut Oid,
    expand_variadic: bool,
    expand_defaults: bool,
    include_out_arguments: bool,
    funcid: *mut Oid,         /* return value */
    rettype: *mut Oid,        /* return value */
    retset: *mut bool,        /* return value */
    nvargs: *mut c_int,       /* return value */
    vatype: *mut Oid,         /* return value */
    true_typeids: *mut *mut Oid, /* return value */
    argdefaults: *mut *mut List, /* optional return value */
) -> FuncDetailCode {
    let raw_candidates: FuncCandidateList;
    let mut best_candidate: FuncCandidateList;

    /* initialize output arguments to silence compiler warnings */
    *funcid = InvalidOid;
    *rettype = InvalidOid;
    *retset = false;
    *nvargs = 0;
    *vatype = InvalidOid;
    *true_typeids = core::ptr::null_mut();
    if !argdefaults.is_null() {
        *argdefaults = NIL;
    }

    /* Get list of possible candidates from namespace search */
    raw_candidates = FuncnameGetCandidates(funcname, nargs, fargnames,
                                           expand_variadic, expand_defaults,
                                           include_out_arguments, false);

    /*
     * Quickly check if there is an exact match to the input datatypes (there
     * can be only one)
     */
    best_candidate = raw_candidates;
    while !best_candidate.is_null() {
        /* if nargs==0, argtypes can be null; don't pass that to memcmp */
        if nargs == 0
            || core::slice::from_raw_parts(argtypes, nargs as usize)
               == core::slice::from_raw_parts((*best_candidate).args.as_ptr(), nargs as usize)
        {
            break;
        }
        best_candidate = (*best_candidate).next;
    }

    if best_candidate.is_null() {
        /*
         * If we didn't find an exact match, next consider the possibility
         * that this is really a type-coercion request: a single-argument
         * function call where the function name is a type name.  If so, and
         * if the coercion path is RELABELTYPE or COERCEVIAIO, then go ahead
         * and treat the "function call" as a coercion.
         *
         * This interpretation needs to be given higher priority than
         * interpretations involving a type coercion followed by a function
         * call, otherwise we can produce surprising results. For example, we
         * want "text(varchar)" to be interpreted as a simple coercion, not as
         * "text(name(varchar))" which the code below this point is entirely
         * capable of selecting.
         *
         * We also treat a coercion of a previously-unknown-type literal
         * constant to a specific type this way.
         *
         * The reason we reject COERCION_PATH_FUNC here is that we expect the
         * cast implementation function to be named after the target type.
         * Thus the function will be found by normal lookup if appropriate.
         *
         * The reason we reject COERCION_PATH_ARRAYCOERCE is mainly that you
         * can't write "foo[] (something)" as a function call.  In theory
         * someone might want to invoke it as "_foo (something)" but we have
         * never supported that historically, so we can insist that people
         * write it as a normal cast instead.
         *
         * We also reject the specific case of COERCEVIAIO for a composite
         * source type and a string-category target type.  This is a case that
         * find_coercion_pathway() allows by default, but experience has shown
         * that it's too commonly invoked by mistake.  So, again, insist that
         * people use cast syntax if they want to do that.
         *
         * NB: it's important that this code does not exceed what coerce_type
         * can do, because the caller will try to apply coerce_type if we
         * return FUNCDETAIL_COERCION.  If we return that result for something
         * coerce_type can't handle, we'll cause infinite recursion between
         * this module and coerce_type!
         */
        if nargs == 1 && !fargs.is_null() && fargnames.is_null() {
            let target_type: Oid = FuncNameAsType(funcname);

            if OidIsValid(target_type) {
                let source_type: Oid = *argtypes.add(0);
                let arg1: *mut Node = linitial(fargs) as *mut Node;
                let iscoercion: bool;

                if source_type == UNKNOWNOID && IsA!(arg1, T_Const) {
                    /* always treat typename('literal') as coercion */
                    iscoercion = true;
                } else {
                    let mut cpathtype: CoercionPathType = COERCION_PATH_NONE;
                    let mut cfuncid: Oid = InvalidOid;

                    cpathtype = find_coercion_pathway(target_type, source_type,
                                                      COERCION_EXPLICIT,
                                                      &mut cfuncid);
                    iscoercion = match cpathtype {
                        COERCION_PATH_RELABELTYPE => true,
                        COERCION_PATH_COERCEVIAIO => {
                            if (source_type == RECORDOID || ISCOMPLEX!(source_type))
                                && TypeCategory(target_type) == TYPCATEGORY_STRING
                            {
                                false
                            } else {
                                true
                            }
                        }
                        _ => false,
                    };
                }

                if iscoercion {
                    /* Treat it as a type coercion */
                    *funcid = InvalidOid;
                    *rettype = target_type;
                    *retset = false;
                    *nvargs = 0;
                    *vatype = InvalidOid;
                    *true_typeids = argtypes;
                    return FUNCDETAIL_COERCION;
                }
            }
        }

        /*
         * didn't find an exact match, so now try to match up candidates...
         */
        if !raw_candidates.is_null() {
            let mut current_candidates: FuncCandidateList = core::ptr::null_mut();
            let ncandidates: c_int;

            ncandidates = func_match_argtypes(nargs, argtypes,
                                              raw_candidates,
                                              &mut current_candidates);

            /* one match only? then run with it... */
            if ncandidates == 1 {
                best_candidate = current_candidates;
            }
            /*
             * multiple candidates? then better decide or throw an error...
             */
            else if ncandidates > 1 {
                best_candidate = func_select_candidate(nargs, argtypes,
                                                       current_candidates);

                /*
                 * If we were able to choose a best candidate, we're done.
                 * Otherwise, ambiguous function call.
                 */
                if best_candidate.is_null() {
                    return FUNCDETAIL_MULTIPLE;
                }
            }
        }
    }

    if !best_candidate.is_null() {
        let ftup: HeapTuple;
        let pform: Form_pg_proc;
        let result: FuncDetailCode;

        /*
         * If processing named args or expanding variadics or defaults, the
         * "best candidate" might represent multiple equivalently good
         * functions; treat this case as ambiguous.
         */
        if !OidIsValid((*best_candidate).oid) {
            return FUNCDETAIL_MULTIPLE;
        }

        /*
         * We disallow VARIADIC with named arguments unless the last argument
         * (the one with VARIADIC attached) actually matched the variadic
         * parameter.  This is mere pedantry, really, but some folks insisted.
         */
        if !fargnames.is_null() && !expand_variadic && nargs > 0
            && !(*best_candidate).argnumbers.is_null()
            && *(*best_candidate).argnumbers.add((nargs - 1) as usize) != (nargs - 1)
        {
            return FUNCDETAIL_NOTFOUND;
        }

        *funcid = (*best_candidate).oid;
        *nvargs = (*best_candidate).nvargs as c_int;
        *true_typeids = (*best_candidate).args.as_mut_ptr();

        /*
         * If processing named args, return actual argument positions into
         * NamedArgExpr nodes in the fargs list.  This is a bit ugly but not
         * worth the extra notation needed to do it differently.
         */
        if !(*best_candidate).argnumbers.is_null() {
            let mut idx: c_int = 0;
            let mut lc: *mut ListCell = list_head(fargs);
            while !lc.is_null() {
                let na = *(lc as *mut *mut Node) as *mut NamedArgExpr;
                if IsA!(na as *const Node, T_NamedArgExpr) {
                    (*na).argnumber = *(*best_candidate).argnumbers.add(idx as usize);
                }
                idx += 1;
                lc = lnext(fargs, lc);
            }
        }

        ftup = SearchSysCache1(PROCOID, ObjectIdGetDatum((*best_candidate).oid));
        if !HeapTupleIsValid(ftup) { /* should not happen */
            elog!(ERROR, "cache lookup failed for function {}",
                  (*best_candidate).oid);
        }
        pform = GETSTRUCT(ftup) as Form_pg_proc;
        *rettype = (*pform).prorettype;
        *retset = (*pform).proretset;
        *vatype = (*pform).provariadic;

        /* fetch default args if caller wants 'em */
        if !argdefaults.is_null() && (*best_candidate).ndargs > 0 {
            let proargdefaults: Datum;
            let str_: *mut c_char;
            let mut defaults: *mut List;

            /* shouldn't happen, FuncnameGetCandidates messed up */
            if (*best_candidate).ndargs > (*pform).pronargdefaults as c_int {
                elog!(ERROR, "not enough default arguments");
            }

            proargdefaults = SysCacheGetAttrNotNull(PROCOID, ftup,
                                                    Anum_pg_proc_proargdefaults);
            str_ = TextDatumGetCString(proargdefaults);
            defaults = castNode!(List, T_List, stringToNode(str_)) as *mut List;
            pfree(str_ as *mut c_void);

            /* Delete any unused defaults from the returned list */
            if !(*best_candidate).argnumbers.is_null() {
                /*
                 * This is a bit tricky in named notation, since the supplied
                 * arguments could replace any subset of the defaults.  We
                 * work by making a bitmapset of the argnumbers of defaulted
                 * arguments, then scanning the defaults list and selecting
                 * the needed items.  (This assumes that defaulted arguments
                 * should be supplied in their positional order.)
                 */
                let mut defargnumbers: *mut Bitmapset = core::ptr::null_mut();
                let firstdefarg: *mut c_int;
                let mut newdefaults: *mut List = NIL;
                let mut lc: *mut ListCell;
                let mut i: c_int;

                firstdefarg = (*best_candidate).argnumbers
                    .add(((*best_candidate).nargs - (*best_candidate).ndargs) as usize);
                i = 0;
                while i < (*best_candidate).ndargs {
                    defargnumbers = bms_add_member(defargnumbers,
                                                   *firstdefarg.add(i as usize));
                    i += 1;
                }
                i = (*best_candidate).nominalnargs - (*pform).pronargdefaults as c_int;
                lc = list_head(defaults);
                while !lc.is_null() {
                    if bms_is_member(i, defargnumbers) {
                        newdefaults = lappend(newdefaults, *(lc as *mut *mut c_void));
                    }
                    i += 1;
                    lc = lnext(defaults, lc);
                }
                debug_assert!(list_length(newdefaults) == (*best_candidate).ndargs);
                bms_free(defargnumbers);
                *argdefaults = newdefaults;
            } else {
                /*
                 * Defaults for positional notation are lots easier; just
                 * remove any unwanted ones from the front.
                 */
                let ndelete: c_int;

                ndelete = list_length(defaults) - (*best_candidate).ndargs;
                if ndelete > 0 {
                    defaults = list_delete_first_n(defaults, ndelete);
                }
                *argdefaults = defaults;
            }
        }

        result = match (*pform).prokind {
            PROKIND_AGGREGATE => FUNCDETAIL_AGGREGATE,
            PROKIND_FUNCTION  => FUNCDETAIL_NORMAL,
            PROKIND_PROCEDURE => FUNCDETAIL_PROCEDURE,
            PROKIND_WINDOW    => FUNCDETAIL_WINDOWFUNC,
            _  => {
                elog!(ERROR, "unrecognized prokind: {}", (*pform).prokind as u8 as char);
                FUNCDETAIL_NORMAL /* keep compiler quiet */
            }
        };

        ReleaseSysCache(ftup);
        return result;
    }

    FUNCDETAIL_NOTFOUND
}

/*
 * unify_hypothetical_args()
 *
 * Ensure that each hypothetical direct argument of a hypothetical-set
 * aggregate has the same type as the corresponding aggregated argument.
 * Modify the expressions in the fargs list, if necessary, and update
 * actual_arg_types[].
 *
 * If the agg declared its args non-ANY (even ANYELEMENT), we need only a
 * sanity check that the declared types match; make_fn_arguments will coerce
 * the actual arguments to match the declared ones.  But if the declaration
 * is ANY, nothing will happen in make_fn_arguments, so we need to fix any
 * mismatch here.  We use the same type resolution logic as UNION etc.
 */
unsafe fn unify_hypothetical_args(
    pstate: *mut ParseState,
    fargs: *mut List,
    numAggregatedArgs: c_int,
    actual_arg_types: *mut Oid,
    declared_arg_types: *mut Oid,
) {
    let numDirectArgs: c_int;
    let numNonHypotheticalArgs: c_int;
    let mut hargpos: c_int;

    numDirectArgs = list_length(fargs) - numAggregatedArgs;
    numNonHypotheticalArgs = numDirectArgs - numAggregatedArgs;
    /* safety check (should only trigger with a misdeclared agg) */
    if numNonHypotheticalArgs < 0 {
        elog!(ERROR, "incorrect number of arguments to hypothetical-set aggregate");
    }

    /* Check each hypothetical arg and corresponding aggregated arg */
    hargpos = numNonHypotheticalArgs;
    while hargpos < numDirectArgs {
        let aargpos: c_int = numDirectArgs + (hargpos - numNonHypotheticalArgs);
        let harg: *mut ListCell = list_nth_cell(fargs, hargpos);
        let aarg: *mut ListCell = list_nth_cell(fargs, aargpos);
        let commontype: Oid;
        let commontypmod: i32;

        /* A mismatch means AggregateCreate didn't check properly ... */
        if *declared_arg_types.add(hargpos as usize) != *declared_arg_types.add(aargpos as usize) {
            elog!(ERROR, "hypothetical-set aggregate has inconsistent declared argument types");
        }

        /* No need to unify if make_fn_arguments will coerce */
        if *declared_arg_types.add(hargpos as usize) != ANYOID {
            hargpos += 1;
            continue;
        }

        /*
         * Select common type, giving preference to the aggregated argument's
         * type (we'd rather coerce the direct argument once than coerce all
         * the aggregated values).
         */
        commontype = select_common_type(pstate,
                                        list_make2!(*(aarg as *mut *mut c_void),
                                                    *(harg as *mut *mut c_void)),
                                        c"WITHIN GROUP".as_ptr(),
                                        core::ptr::null_mut());
        commontypmod = select_common_typmod(pstate,
                                            list_make2!(*(aarg as *mut *mut c_void),
                                                        *(harg as *mut *mut c_void)),
                                            commontype);

        /*
         * Perform the coercions.  We don't need to worry about NamedArgExprs
         * here because they aren't supported with aggregates.
         */
        *(harg as *mut *mut c_void) = coerce_type(pstate,
                                                   *(harg as *mut *mut Node),
                                                   *actual_arg_types.add(hargpos as usize),
                                                   commontype, commontypmod,
                                                   COERCION_IMPLICIT,
                                                   COERCE_IMPLICIT_CAST,
                                                   -1) as *mut c_void;
        *actual_arg_types.add(hargpos as usize) = commontype;
        *(aarg as *mut *mut c_void) = coerce_type(pstate,
                                                   *(aarg as *mut *mut Node),
                                                   *actual_arg_types.add(aargpos as usize),
                                                   commontype, commontypmod,
                                                   COERCION_IMPLICIT,
                                                   COERCE_IMPLICIT_CAST,
                                                   -1) as *mut c_void;
        *actual_arg_types.add(aargpos as usize) = commontype;
        hargpos += 1;
    }
}


/*
 * make_fn_arguments()
 *
 * Given the actual argument expressions for a function, and the desired
 * input types for the function, add any necessary typecasting to the
 * expression tree.  Caller should already have verified that casting is
 * allowed.
 *
 * Caution: given argument list is modified in-place.
 *
 * As with coerce_type, pstate may be NULL if no special unknown-Param
 * processing is wanted.
 */
pub unsafe fn make_fn_arguments(
    pstate: *mut ParseState,
    fargs: *mut List,
    actual_arg_types: *mut Oid,
    declared_arg_types: *mut Oid,
) {
    let mut current_fargs: *mut ListCell;
    let mut i: c_int = 0;

    current_fargs = list_head(fargs);
    while !current_fargs.is_null() {
        /* types don't match? then force coercion using a function call... */
        if *actual_arg_types.add(i as usize) != *declared_arg_types.add(i as usize) {
            let mut node: *mut Node = *(current_fargs as *mut *mut Node);

            /*
             * If arg is a NamedArgExpr, coerce its input expr instead --- we
             * want the NamedArgExpr to stay at the top level of the list.
             */
            if IsA!(node, T_NamedArgExpr) {
                let na = node as *mut NamedArgExpr;

                node = coerce_type(pstate,
                                   (*na).arg as *mut Node,
                                   *actual_arg_types.add(i as usize),
                                   *declared_arg_types.add(i as usize), -1,
                                   COERCION_IMPLICIT,
                                   COERCE_IMPLICIT_CAST,
                                   -1);
                (*na).arg = node as *mut Expr;
            } else {
                node = coerce_type(pstate,
                                   node,
                                   *actual_arg_types.add(i as usize),
                                   *declared_arg_types.add(i as usize), -1,
                                   COERCION_IMPLICIT,
                                   COERCE_IMPLICIT_CAST,
                                   -1);
                *(current_fargs as *mut *mut c_void) = node as *mut c_void;
            }
        }
        i += 1;
        current_fargs = lnext(fargs, current_fargs);
    }
}

/*
 * FuncNameAsType -
 *    convenience routine to see if a function name matches a type name
 *
 * Returns the OID of the matching type, or InvalidOid if none.  We ignore
 * shell types and complex types.
 */
unsafe fn FuncNameAsType(funcname: *mut List) -> Oid {
    let result: Oid;
    let typtup: HeapTuple; // Type alias in C is HeapTuple

    /*
     * temp_ok=false protects the <refsect1 id="sql-createfunction-security">
     * contract for writing SECURITY DEFINER functions safely.
     */
    let typtup = LookupTypeNameExtended(core::ptr::null_mut(),
                                        makeTypeNameFromNameList(funcname),
                                        core::ptr::null_mut(), false, false);
    if typtup.is_null() {
        return InvalidOid;
    }

    let pg_type_form = GETSTRUCT(typtup) as crate::catalog::pg_type::Form_pg_type;
    if (*pg_type_form).typisdefined && !OidIsValid(typeTypeRelid(typtup)) {
        result = typeTypeId(typtup);
    } else {
        result = InvalidOid;
    }

    ReleaseSysCache(typtup);
    result
}

/*
 * ParseComplexProjection -
 *    handles function calls with a single argument that is of complex type.
 *    If the function call is actually a column projection, return a suitably
 *    transformed expression tree.  If not, return NULL.
 */
unsafe fn ParseComplexProjection(
    pstate: *mut ParseState,
    funcname: *const c_char,
    first_arg: *mut Node,
    location: c_int,
) -> *mut Node {
    let mut i: c_int;

    /*
     * Special case for whole-row Vars so that we can resolve (foo.*).bar even
     * when foo is a reference to a subselect, join, or RECORD function. A
     * bonus is that we avoid generating an unnecessary FieldSelect; our
     * result can omit the whole-row Var and just be a Var for the selected
     * field.
     *
     * This case could be handled by expandRecordVariable, but it's more
     * efficient to do it this way when possible.
     */
    if IsA!(first_arg, T_Var)
        && (*(first_arg as *mut Var)).varattno == InvalidAttrNumber
    {
        let var = first_arg as *mut Var;
        let nsitem = GetNSItemByRangeTablePosn(pstate, (*var).varno, (*var).varlevelsup as c_int);
        /* Return a Var if funcname matches a column, else NULL */
        return scanNSItemForColumn(pstate, nsitem,
                                   (*var).varlevelsup as c_int,
                                   funcname, location);
    }

    /*
     * Else do it the hard way with get_expr_result_tupdesc().
     *
     * If it's a Var of type RECORD, we have to work even harder: we have to
     * find what the Var refers to, and pass that to get_expr_result_tupdesc.
     * That task is handled by expandRecordVariable().
     */
    /* TODO(pg-port): expandRecordVariable and get_expr_result_tupdesc not yet ported;
     * stubs returning null are used here so projection on RECORD vars won't work. */
    let tupdesc: *mut crate::access::common::tupdesc::TupleDescData = if IsA!(first_arg, T_Var)
        && (*(first_arg as *mut Var)).vartype == RECORDOID
    {
        core::ptr::null_mut() // expandRecordVariable(pstate, first_arg as *mut Var, 0)
    } else {
        core::ptr::null_mut() // get_expr_result_tupdesc(first_arg, true)
    };
    if tupdesc.is_null() {
        return core::ptr::null_mut(); /* unresolvable RECORD type */
    }

    /* NOTE: unreachable with null tupdesc stubs above, kept for completeness */
    let tupdesc: crate::access::common::tupdesc::TupleDescData = unreachable!();
}


/*
 * funcname_signature_string
 *      Build a string representing a function name, including arg types.
 *      The result is something like "foo(integer)".
 *
 * If argnames isn't NIL, it is a list of C strings representing the actual
 * arg names for the last N arguments.  This must be considered part of the
 * function signature too, when dealing with named-notation function calls.
 *
 * This is typically used in the construction of function-not-found error
 * messages.
 */
pub unsafe fn funcname_signature_string(
    funcname: *const c_char,
    nargs: c_int,
    argnames: *mut List,
    argtypes: *const Oid,
) -> *const c_char {
    let mut argbuf = core::mem::zeroed::<StringInfoData>();
    let argbuf_ptr: StringInfo = &mut argbuf;
    let numposargs: c_int;
    let mut lc: *mut ListCell;
    let mut i: c_int;

    initStringInfo(argbuf_ptr);

    appendStringInfo!(argbuf_ptr, "{}(", cstr_to_str(funcname));

    numposargs = nargs - list_length(argnames);
    lc = list_head(argnames);

    i = 0;
    while i < nargs {
        if i > 0 {
            appendStringInfoString(argbuf_ptr, c", ".as_ptr());
        }
        if i >= numposargs {
            appendStringInfo!(argbuf_ptr, "{} => ",
                              cstr_to_str(*(lc as *mut *mut c_char)));
            lc = lnext(argnames, lc);
        }
        appendStringInfoString(argbuf_ptr, format_type_be(*argtypes.add(i as usize)));
        i += 1;
    }

    appendStringInfoChar(argbuf_ptr, b')' as c_char);

    (*argbuf_ptr).data as *const c_char /* return palloc'd string buffer */
}

/*
 * func_signature_string
 *      As above, but function name is passed as a qualified name list.
 */
pub unsafe fn func_signature_string(
    funcname: *mut List,
    nargs: c_int,
    argnames: *mut List,
    argtypes: *const Oid,
) -> *const c_char {
    funcname_signature_string(NameListToString(funcname),
                              nargs, argnames, argtypes)
}

/*
 * LookupFuncNameInternal
 *      Workhorse for LookupFuncName/LookupFuncWithArgs
 *
 * In an error situation, e.g. can't find the function, then we return
 * InvalidOid and set *lookupError to indicate what went wrong.
 *
 * Possible errors:
 *  FUNCLOOKUP_NOSUCHFUNC: we can't find a function of this name.
 *  FUNCLOOKUP_AMBIGUOUS: more than one function matches.
 */
unsafe fn LookupFuncNameInternal(
    objtype: ObjectType,
    funcname: *mut List,
    nargs: c_int,
    argtypes: *const Oid,
    include_out_arguments: bool,
    missing_ok: bool,
    lookupError: *mut FuncLookupError,
) -> Oid {
    let mut result: Oid = InvalidOid;
    let mut clist: FuncCandidateList;

    /* NULL argtypes allowed for nullary functions only */
    debug_assert!(!argtypes.is_null() || nargs == 0);

    /* Always set *lookupError, to forestall uninitialized-variable warnings */
    *lookupError = FUNCLOOKUP_NOSUCHFUNC;

    /* Get list of candidate objects */
    clist = FuncnameGetCandidates(funcname, nargs, NIL, false, false,
                                  include_out_arguments, missing_ok);

    /* Scan list for a match to the arg types (if specified) and the objtype */
    while !clist.is_null() {
        /* Check arg type match, if specified */
        if nargs >= 0 {
            /* if nargs==0, argtypes can be null; don't pass that to memcmp */
            if nargs > 0 {
                if core::slice::from_raw_parts(argtypes, nargs as usize)
                   != core::slice::from_raw_parts((*clist).args.as_ptr(), nargs as usize)
                {
                    clist = (*clist).next;
                    continue;
                }
            }
        }

        /* Check for duplicates reported by FuncnameGetCandidates */
        if !OidIsValid((*clist).oid) {
            *lookupError = FUNCLOOKUP_AMBIGUOUS;
            return InvalidOid;
        }

        /* Check objtype match, if specified */
        match objtype {
            OBJECT_FUNCTION | OBJECT_AGGREGATE => {
                /* Ignore procedures */
                if get_func_prokind((*clist).oid) == PROKIND_PROCEDURE {
                    clist = (*clist).next;
                    continue;
                }
            }
            OBJECT_PROCEDURE => {
                /* Ignore non-procedures */
                if get_func_prokind((*clist).oid) != PROKIND_PROCEDURE {
                    clist = (*clist).next;
                    continue;
                }
            }
            OBJECT_ROUTINE => {
                /* no restriction */
            }
            _ => {
                debug_assert!(false);
            }
        }

        /* Check for multiple matches */
        if OidIsValid(result) {
            *lookupError = FUNCLOOKUP_AMBIGUOUS;
            return InvalidOid;
        }

        /* OK, we have a candidate */
        result = (*clist).oid;
        clist = (*clist).next;
    }

    result
}

/*
 * LookupFuncName
 *
 * Given a possibly-qualified function name and optionally a set of argument
 * types, look up the function.  Pass nargs == -1 to indicate that the number
 * and types of the arguments are unspecified (this is NOT the same as
 * specifying that there are no arguments).
 *
 * If the function name is not schema-qualified, it is sought in the current
 * namespace search path.
 *
 * If the function is not found, we return InvalidOid if missing_ok is true,
 * else raise an error.
 *
 * If nargs == -1 and multiple functions are found matching this function name
 * we will raise an ambiguous-function error, regardless of what missing_ok is
 * set to.
 *
 * Only functions will be found; procedures will be ignored even if they
 * match the name and argument types.  (However, we don't trouble to reject
 * aggregates or window functions here.)
 */
pub unsafe fn LookupFuncName(
    funcname: *mut List,
    nargs: c_int,
    argtypes: *const Oid,
    missing_ok: bool,
) -> Oid {
    let funcoid: Oid;
    let mut lookup_error = FUNCLOOKUP_NOSUCHFUNC;

    funcoid = LookupFuncNameInternal(OBJECT_FUNCTION,
                                     funcname, nargs, argtypes,
                                     false, missing_ok,
                                     &mut lookup_error);

    if OidIsValid(funcoid) {
        return funcoid;
    }

    match lookup_error {
        FUNCLOOKUP_NOSUCHFUNC => {
            /* Let the caller deal with it when missing_ok is true */
            if missing_ok {
                return InvalidOid;
            }

            if nargs < 0 {
                ereport!(ERROR,
                    errmsg!("could not find a function named \"{}\"",
                            cstr_to_str(NameListToString(funcname)))
                    /* C also: errcode(ERRCODE_UNDEFINED_FUNCTION) */
                );
            } else {
                ereport!(ERROR,
                    errmsg!("function {} does not exist",
                            cstr_to_str(func_signature_string(funcname, nargs,
                                                              NIL, argtypes)))
                    /* C also: errcode(ERRCODE_UNDEFINED_FUNCTION) */
                );
            }
        }
        FUNCLOOKUP_AMBIGUOUS => {
            /* Raise an error regardless of missing_ok */
            ereport!(ERROR,
                errmsg!("function name \"{}\" is not unique",
                        cstr_to_str(NameListToString(funcname)))
                /* C also: errcode(ERRCODE_AMBIGUOUS_FUNCTION),
                   errhint("Specify the argument list to select the function unambiguously.") */
            );
        }
    }

    InvalidOid /* Keep compiler quiet */
}

/*
 * LookupFuncWithArgs
 *
 * Like LookupFuncName, but the argument types are specified by an
 * ObjectWithArgs node.  Also, this function can check whether the result is a
 * function, procedure, or aggregate, based on the objtype argument.  Pass
 * OBJECT_ROUTINE to accept any of them.
 *
 * For historical reasons, we also accept aggregates when looking for a
 * function.
 *
 * When missing_ok is true we don't generate any error for missing objects and
 * return InvalidOid.  Other types of errors can still be raised, regardless
 * of the value of missing_ok.
 */
pub unsafe fn LookupFuncWithArgs(
    objtype: ObjectType,
    func: *mut ObjectWithArgs,
    missing_ok: bool,
) -> Oid {
    let mut argoids: [Oid; FUNC_MAX_ARGS as usize] = [0; FUNC_MAX_ARGS as usize];
    let argcount: c_int;
    let nargs: c_int;
    let mut i: c_int;
    let mut args_item: *mut ListCell;
    let mut oid: Oid;
    let mut lookup_error = FUNCLOOKUP_NOSUCHFUNC;

    debug_assert!(objtype == OBJECT_AGGREGATE
               || objtype == OBJECT_FUNCTION
               || objtype == OBJECT_PROCEDURE
               || objtype == OBJECT_ROUTINE);

    argcount = list_length((*func).objargs);
    if argcount > FUNC_MAX_ARGS as c_int {
        if objtype == OBJECT_PROCEDURE {
            ereport!(ERROR,
                errmsg!("procedures cannot have more than {} arguments", FUNC_MAX_ARGS)
                /* C also: errcode(ERRCODE_TOO_MANY_ARGUMENTS), errmsg_plural */
            );
        } else {
            ereport!(ERROR,
                errmsg!("functions cannot have more than {} arguments", FUNC_MAX_ARGS)
                /* C also: errcode(ERRCODE_TOO_MANY_ARGUMENTS), errmsg_plural */
            );
        }
    }

    /*
     * First, perform a lookup considering only input arguments (traditional
     * Postgres rules).
     */
    i = 0;
    args_item = list_head((*func).objargs);
    while !args_item.is_null() {
        let t = *(args_item as *mut *mut TypeName);
        argoids[i as usize] = LookupTypeNameOid(core::ptr::null_mut(), t, missing_ok);
        if !OidIsValid(argoids[i as usize]) {
            return InvalidOid; /* missing_ok must be true */
        }
        i += 1;
        args_item = lnext((*func).objargs, args_item);
    }

    /*
     * Set nargs for LookupFuncNameInternal. It expects -1 to mean no args
     * were specified.
     */
    nargs = if (*func).args_unspecified { -1 } else { argcount };

    /*
     * In args_unspecified mode, also tell LookupFuncNameInternal to consider
     * the object type, since there seems no reason not to.  However, if we
     * have an argument list, disable the objtype check, because we'd rather
     * complain about "object is of wrong type" than "object doesn't exist".
     * (Note that with args, FuncnameGetCandidates will have ensured there's
     * only one argtype match, so we're not risking an ambiguity failure via
     * this choice.)
     */
    oid = LookupFuncNameInternal(
        if (*func).args_unspecified { objtype } else { OBJECT_ROUTINE },
        (*func).objname, nargs, argoids.as_ptr(),
        false, missing_ok,
        &mut lookup_error);

    /*
     * If PROCEDURE or ROUTINE was specified, and we have an argument list
     * that contains no parameter mode markers, and we didn't already discover
     * that there's ambiguity, perform a lookup considering all arguments.
     * (Note: for a zero-argument procedure, or in args_unspecified mode, the
     * normal lookup is sufficient; so it's OK to require non-NIL objfuncargs
     * to perform this lookup.)
     */
    if (objtype == OBJECT_PROCEDURE || objtype == OBJECT_ROUTINE)
        && !(*func).objfuncargs.is_null()
        && list_length((*func).objfuncargs) > 0
        && lookup_error != FUNCLOOKUP_AMBIGUOUS
    {
        let mut have_param_mode = false;

        /*
         * Check for non-default parameter mode markers.  If there are any,
         * then the command does not conform to SQL-spec syntax, so we may
         * assume that the traditional Postgres lookup method of considering
         * only input parameters is sufficient.  (Note that because the spec
         * doesn't have OUT arguments for functions, we also don't need this
         * hack in FUNCTION or AGGREGATE mode.)
         */
        args_item = list_head((*func).objfuncargs);
        while !args_item.is_null() {
            let fp = *(args_item as *mut *mut FunctionParameter);
            if (*fp).mode != crate::nodes::parsenodes::FunctionParameterMode::FUNC_PARAM_DEFAULT {
                have_param_mode = true;
                break;
            }
            args_item = lnext((*func).objfuncargs, args_item);
        }

        if !have_param_mode {
            let poid: Oid;

            /* Without mode marks, objargs surely includes all params */
            debug_assert!(list_length((*func).objfuncargs) == argcount);

            /* For objtype == OBJECT_PROCEDURE, we can ignore non-procedures */
            poid = LookupFuncNameInternal(objtype, (*func).objname,
                                          argcount, argoids.as_ptr(),
                                          true, missing_ok,
                                          &mut lookup_error);

            /* Combine results, handling ambiguity */
            if OidIsValid(poid) {
                if OidIsValid(oid) && oid != poid {
                    /* oops, we got hits both ways, on different objects */
                    oid = InvalidOid;
                    lookup_error = FUNCLOOKUP_AMBIGUOUS;
                } else {
                    oid = poid;
                }
            } else if lookup_error == FUNCLOOKUP_AMBIGUOUS {
                oid = InvalidOid;
            }
        }
    }

    if OidIsValid(oid) {
        /*
         * Even if we found the function, perform validation that the objtype
         * matches the prokind of the found function.  For historical reasons
         * we allow the objtype of FUNCTION to include aggregates and window
         * functions; but we draw the line if the object is a procedure.  That
         * is a new enough feature that this historical rule does not apply.
         *
         * (This check is partially redundant with the objtype check in
         * LookupFuncNameInternal; but not entirely, since we often don't tell
         * LookupFuncNameInternal to apply that check at all.)
         */
        match objtype {
            OBJECT_FUNCTION => {
                /* Only complain if it's a procedure. */
                if get_func_prokind(oid) == PROKIND_PROCEDURE {
                    ereport!(ERROR,
                        errmsg!("{} is not a function",
                                cstr_to_str(func_signature_string((*func).objname, argcount,
                                                                  NIL, argoids.as_ptr())))
                        /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */
                    );
                }
            }
            OBJECT_PROCEDURE => {
                /* Reject if found object is not a procedure. */
                if get_func_prokind(oid) != PROKIND_PROCEDURE {
                    ereport!(ERROR,
                        errmsg!("{} is not a procedure",
                                cstr_to_str(func_signature_string((*func).objname, argcount,
                                                                  NIL, argoids.as_ptr())))
                        /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */
                    );
                }
            }
            OBJECT_AGGREGATE => {
                /* Reject if found object is not an aggregate. */
                if get_func_prokind(oid) != PROKIND_AGGREGATE {
                    ereport!(ERROR,
                        errmsg!("function {} is not an aggregate",
                                cstr_to_str(func_signature_string((*func).objname, argcount,
                                                                  NIL, argoids.as_ptr())))
                        /* C also: errcode(ERRCODE_WRONG_OBJECT_TYPE) */
                    );
                }
            }
            _ => {
                /* OBJECT_ROUTINE accepts anything. */
            }
        }

        return oid; /* All good */
    } else {
        /* Deal with cases where the lookup failed */
        match lookup_error {
            FUNCLOOKUP_NOSUCHFUNC => {
                /* Suppress no-such-func errors when missing_ok is true */
                if missing_ok {
                    /* fall through to return InvalidOid */
                } else {
                    match objtype {
                        OBJECT_PROCEDURE => {
                            if (*func).args_unspecified {
                                ereport!(ERROR,
                                    errmsg!("could not find a procedure named \"{}\"",
                                            cstr_to_str(NameListToString((*func).objname)))
                                );
                            } else {
                                ereport!(ERROR,
                                    errmsg!("procedure {} does not exist",
                                            cstr_to_str(func_signature_string((*func).objname,
                                                                              argcount, NIL,
                                                                              argoids.as_ptr())))
                                );
                            }
                        }
                        OBJECT_AGGREGATE => {
                            if (*func).args_unspecified {
                                ereport!(ERROR,
                                    errmsg!("could not find an aggregate named \"{}\"",
                                            cstr_to_str(NameListToString((*func).objname)))
                                );
                            } else if argcount == 0 {
                                ereport!(ERROR,
                                    errmsg!("aggregate {}(*) does not exist",
                                            cstr_to_str(NameListToString((*func).objname)))
                                );
                            } else {
                                ereport!(ERROR,
                                    errmsg!("aggregate {} does not exist",
                                            cstr_to_str(func_signature_string((*func).objname,
                                                                              argcount, NIL,
                                                                              argoids.as_ptr())))
                                );
                            }
                        }
                        _ => {
                            /* FUNCTION and ROUTINE */
                            if (*func).args_unspecified {
                                ereport!(ERROR,
                                    errmsg!("could not find a function named \"{}\"",
                                            cstr_to_str(NameListToString((*func).objname)))
                                );
                            } else {
                                ereport!(ERROR,
                                    errmsg!("function {} does not exist",
                                            cstr_to_str(func_signature_string((*func).objname,
                                                                              argcount, NIL,
                                                                              argoids.as_ptr())))
                                );
                            }
                        }
                    }
                }
            }
            FUNCLOOKUP_AMBIGUOUS => {
                match objtype {
                    OBJECT_FUNCTION => {
                        ereport!(ERROR,
                            errmsg!("function name \"{}\" is not unique",
                                    cstr_to_str(NameListToString((*func).objname)))
                            /* C also: errcode(ERRCODE_AMBIGUOUS_FUNCTION),
                               conditional errhint based on args_unspecified */
                        );
                    }
                    OBJECT_PROCEDURE => {
                        ereport!(ERROR,
                            errmsg!("procedure name \"{}\" is not unique",
                                    cstr_to_str(NameListToString((*func).objname)))
                        );
                    }
                    OBJECT_AGGREGATE => {
                        ereport!(ERROR,
                            errmsg!("aggregate name \"{}\" is not unique",
                                    cstr_to_str(NameListToString((*func).objname)))
                        );
                    }
                    OBJECT_ROUTINE | _ => {
                        ereport!(ERROR,
                            errmsg!("routine name \"{}\" is not unique",
                                    cstr_to_str(NameListToString((*func).objname)))
                        );
                    }
                }
            }
        }

        return InvalidOid;
    }
}

/*
 * check_srf_call_placement
 *      Verify that a set-returning function is called in a valid place,
 *      and throw a nice error if not.
 *
 * A side-effect is to set pstate->p_hasTargetSRFs true if appropriate.
 *
 * last_srf should be a copy of pstate->p_last_srf from just before we
 * started transforming the function's arguments.  This allows detection
 * of whether the SRF's arguments contain any SRFs.
 */
pub unsafe fn check_srf_call_placement(
    pstate: *mut ParseState,
    last_srf: *mut Node,
    location: c_int,
) {
    let err: *const c_char;
    let errkind: bool;

    /*
     * Check to see if the set-returning function is in an invalid place
     * within the query.  Basically, we don't allow SRFs anywhere except in
     * the targetlist (which includes GROUP BY/ORDER BY expressions), VALUES,
     * and functions in FROM.
     *
     * For brevity we support two schemes for reporting an error here: set
     * "err" to a custom message, or set "errkind" true if the error context
     * is sufficiently identified by what ParseExprKindName will return, *and*
     * what it will return is just a SQL keyword.  (Otherwise, use a custom
     * message to avoid creating translation problems.)
     */
    let mut err: *const c_char = core::ptr::null();
    let mut errkind = false;

    match (*pstate).p_expr_kind {
        EXPR_KIND_NONE => {
            debug_assert!(false); /* can't happen */
        }
        EXPR_KIND_OTHER => {
            /* Accept SRF here; caller must throw error if wanted */
        }
        EXPR_KIND_JOIN_ON | EXPR_KIND_JOIN_USING => {
            err = c"set-returning functions are not allowed in JOIN conditions".as_ptr();
        }
        EXPR_KIND_FROM_SUBSELECT => {
            /* can't get here, but just in case, throw an error */
            errkind = true;
        }
        EXPR_KIND_FROM_FUNCTION => {
            /* okay, but we don't allow nested SRFs here */
            /* errmsg is chosen to match transformRangeFunction() */
            /* errposition should point to the inner SRF */
            if (*pstate).p_last_srf != last_srf {
                ereport!(ERROR,
                    errmsg!("set-returning functions must appear at top level of FROM")
                    /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED), parser_errposition */
                );
            }
        }
        EXPR_KIND_WHERE => { errkind = true; }
        EXPR_KIND_POLICY => {
            err = c"set-returning functions are not allowed in policy expressions".as_ptr();
        }
        EXPR_KIND_HAVING => { errkind = true; }
        EXPR_KIND_FILTER => { errkind = true; }
        EXPR_KIND_WINDOW_PARTITION | EXPR_KIND_WINDOW_ORDER => {
            /* okay, these are effectively GROUP BY/ORDER BY */
            (*pstate).p_hasTargetSRFs = true;
        }
        EXPR_KIND_WINDOW_FRAME_RANGE
        | EXPR_KIND_WINDOW_FRAME_ROWS
        | EXPR_KIND_WINDOW_FRAME_GROUPS => {
            err = c"set-returning functions are not allowed in window definitions".as_ptr();
        }
        EXPR_KIND_SELECT_TARGET | EXPR_KIND_INSERT_TARGET => {
            /* okay */
            (*pstate).p_hasTargetSRFs = true;
        }
        EXPR_KIND_UPDATE_SOURCE | EXPR_KIND_UPDATE_TARGET => {
            /* disallowed because it would be ambiguous what to do */
            errkind = true;
        }
        EXPR_KIND_GROUP_BY | EXPR_KIND_ORDER_BY => {
            /* okay */
            (*pstate).p_hasTargetSRFs = true;
        }
        EXPR_KIND_DISTINCT_ON => {
            /* okay */
            (*pstate).p_hasTargetSRFs = true;
        }
        EXPR_KIND_LIMIT | EXPR_KIND_OFFSET => { errkind = true; }
        EXPR_KIND_RETURNING | EXPR_KIND_MERGE_RETURNING => { errkind = true; }
        EXPR_KIND_VALUES => {
            /* SRFs are presently not supported by nodeValuesscan.c */
            errkind = true;
        }
        EXPR_KIND_VALUES_SINGLE => {
            /* okay, since we process this like a SELECT tlist */
            (*pstate).p_hasTargetSRFs = true;
        }
        EXPR_KIND_MERGE_WHEN => {
            err = c"set-returning functions are not allowed in MERGE WHEN conditions".as_ptr();
        }
        EXPR_KIND_CHECK_CONSTRAINT | EXPR_KIND_DOMAIN_CHECK => {
            err = c"set-returning functions are not allowed in check constraints".as_ptr();
        }
        EXPR_KIND_COLUMN_DEFAULT | EXPR_KIND_FUNCTION_DEFAULT => {
            err = c"set-returning functions are not allowed in DEFAULT expressions".as_ptr();
        }
        EXPR_KIND_INDEX_EXPRESSION => {
            err = c"set-returning functions are not allowed in index expressions".as_ptr();
        }
        EXPR_KIND_INDEX_PREDICATE => {
            err = c"set-returning functions are not allowed in index predicates".as_ptr();
        }
        EXPR_KIND_STATS_EXPRESSION => {
            err = c"set-returning functions are not allowed in statistics expressions".as_ptr();
        }
        EXPR_KIND_ALTER_COL_TRANSFORM => {
            err = c"set-returning functions are not allowed in transform expressions".as_ptr();
        }
        EXPR_KIND_EXECUTE_PARAMETER => {
            err = c"set-returning functions are not allowed in EXECUTE parameters".as_ptr();
        }
        EXPR_KIND_TRIGGER_WHEN => {
            err = c"set-returning functions are not allowed in trigger WHEN conditions".as_ptr();
        }
        EXPR_KIND_PARTITION_BOUND => {
            err = c"set-returning functions are not allowed in partition bound".as_ptr();
        }
        EXPR_KIND_PARTITION_EXPRESSION => {
            err = c"set-returning functions are not allowed in partition key expressions".as_ptr();
        }
        EXPR_KIND_CALL_ARGUMENT => {
            err = c"set-returning functions are not allowed in CALL arguments".as_ptr();
        }
        EXPR_KIND_COPY_WHERE => {
            err = c"set-returning functions are not allowed in COPY FROM WHERE conditions".as_ptr();
        }
        EXPR_KIND_GENERATED_COLUMN => {
            err = c"set-returning functions are not allowed in column generation expressions".as_ptr();
        }
        EXPR_KIND_CYCLE_MARK => { errkind = true; }
        /*
         * There is intentionally no default: case here, so that the
         * compiler will warn if we add a new ParseExprKind without
         * extending this switch.  If we do see an unrecognized value at
         * runtime, the behavior will be the same as for EXPR_KIND_OTHER,
         * which is sane anyway.
         */
        _ => {}
    }

    if !err.is_null() {
        ereport!(ERROR,
            errmsg!("{}", cstr_to_str(err))
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED), parser_errposition */
        );
    }
    if errkind {
        ereport!(ERROR,
            errmsg!("set-returning functions are not allowed in {}",
                    cstr_to_str(crate::parser::parse_expr::ParseExprKindName((*pstate).p_expr_kind)))
            /* C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED), parser_errposition,
               translator note: %s is name of a SQL construct, eg GROUP BY */
        );
    }
}

/* ---------- small helpers used above ---------- */

/// Rust helper: convert *const c_char to &str (lossy).
#[inline]
unsafe fn cstr_to_str(s: *const c_char) -> &'static str {
    if s.is_null() {
        return "<null>";
    }
    core::ffi::CStr::from_ptr(s).to_str().unwrap_or("<invalid utf8>")
}

/// Rust helper: strcmp wrapper.
#[inline]
unsafe fn libc_strcmp(a: *const c_char, b: *const c_char) -> c_int {
    extern "C" { fn strcmp(a: *const c_char, b: *const c_char) -> c_int; }
    strcmp(a, b)
}
