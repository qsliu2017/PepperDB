//! src/backend/parser/parse_oper.c
//!
//! handle operator things for parser
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

use std::ffi::{c_char, c_int};

use crate::c::{uint32, Size};
use crate::nodes::pg_list::List;
use crate::nodes::nodes::{Node, NodeTag};
use crate::pg_config_manual::NAMEDATALEN;
use crate::postgres_ext::Oid;
use crate::postgres::Datum;

// crate-root #[macro_export] macros used below
use crate::{makeNode, castNode, linitial_node, lsecond_node};

// real definitions (replace local stubs)
use crate::access::htup_details::{HeapTuple, HeapTupleIsValid, GETSTRUCT};

/*
 * The lookup key for the operator lookaside hash table.  Unused bits must be
 * zeroes to ensure hashing works consistently --- in particular, oprname
 * must be zero-padded and any unused entries in search_path must be zero.
 *
 * search_path contains the actual search_path with which the entry was
 * derived (minus temp namespace if any), or else the single specified
 * schema OID if we are looking up an explicitly-qualified operator name.
 *
 * search_path has to be fixed-length since the hashtable code insists on
 * fixed-size keys.  If your search path is longer than that, we just punt
 * and don't cache anything.
 */

/* If your search_path is longer than this, sucks to be you ... */
const MAX_CACHED_PATH_LEN: usize = 16;

#[repr(C)]
struct OprCacheKey {
    oprname: [c_char; NAMEDATALEN], /* operator name */
    left_arg: Oid,                  /* Left input OID, or 0 if prefix op */
    right_arg: Oid,                 /* Right input OID */
    search_path: [Oid; MAX_CACHED_PATH_LEN],
}

#[repr(C)]
struct OprCacheEntry {
    /* the hash lookup key MUST BE FIRST */
    key: OprCacheKey,

    opr_oid: Oid, /* OID of the resolved operator */
}

/*
 * LookupOperName
 *		Given a possibly-qualified operator name and exact input datatypes,
 *		look up the operator.
 *
 * Pass oprleft = InvalidOid for a prefix op.
 *
 * If the operator name is not schema-qualified, it is sought in the current
 * namespace search path.
 *
 * If the operator is not found, we return InvalidOid if noError is true,
 * else raise an error.  pstate and location are used only to report the
 * error position; pass NULL/-1 if not available.
 */
pub unsafe fn LookupOperName(
    pstate: *mut ParseState,
    opername: *mut List,
    oprleft: Oid,
    oprright: Oid,
    noError: bool,
    location: c_int,
) -> Oid {
    let result: Oid;

    result = OpernameGetOprid(opername, oprleft, oprright);
    if OidIsValid(result) {
        return result;
    }

    /* we don't use op_error here because only an exact match is wanted */
    if !noError {
        if !OidIsValid(oprright) {
            ereport!(ERROR, "postfix operators are not supported");
            unreachable!();
        }

        elog!(
            ERROR,
            "operator does not exist: {}",
            CStr_to_str(op_signature_string(opername, oprleft, oprright))
        );
        unreachable!();
    }

    InvalidOid
}

/*
 * LookupOperWithArgs
 *		Like LookupOperName, but the argument types are specified by
 *		a ObjectWithArgs node.
 */
pub unsafe fn LookupOperWithArgs(oper: *mut ObjectWithArgs, noError: bool) -> Oid {
    let oprleft: *mut TypeName;
    let oprright: *mut TypeName;
    let leftoid: Oid;
    let rightoid: Oid;

    Assert!(list_length((*oper).objargs) == 2);
    oprleft = linitial_node!(TypeName, T_TypeName, (*oper).objargs);
    oprright = lsecond_node!(TypeName, T_TypeName, (*oper).objargs);

    if oprleft.is_null() {
        leftoid = InvalidOid;
    } else {
        leftoid = LookupTypeNameOid(std::ptr::null_mut(), oprleft, noError);
    }

    if oprright.is_null() {
        rightoid = InvalidOid;
    } else {
        rightoid = LookupTypeNameOid(std::ptr::null_mut(), oprright, noError);
    }

    LookupOperName(
        std::ptr::null_mut(),
        (*oper).objname,
        leftoid,
        rightoid,
        noError,
        -1,
    )
}

/*
 * get_sort_group_operators - get default sorting/grouping operators for type
 *
 * We fetch the "<", "=", and ">" operators all at once to reduce lookup
 * overhead (knowing that most callers will be interested in at least two).
 * However, a given datatype might have only an "=" operator, if it is
 * hashable but not sortable.  (Other combinations of present and missing
 * operators shouldn't happen, unless the system catalogs are messed up.)
 *
 * If an operator is missing and the corresponding needXX flag is true,
 * throw a standard error message, else return InvalidOid.
 *
 * In addition to the operator OIDs themselves, this function can identify
 * whether the "=" operator is hashable.
 *
 * Callers can pass NULL pointers for any results they don't care to get.
 *
 * Note: the results are guaranteed to be exact or binary-compatible matches,
 * since most callers are not prepared to cope with adding any run-time type
 * coercion steps.
 */
pub unsafe fn get_sort_group_operators(
    argtype: Oid,
    needLT: bool,
    needEQ: bool,
    needGT: bool,
    ltOpr: *mut Oid,
    eqOpr: *mut Oid,
    gtOpr: *mut Oid,
    isHashable: *mut bool,
) {
    let typentry: *mut TypeCacheEntry;
    let cache_flags: c_int;
    let lt_opr: Oid;
    let eq_opr: Oid;
    let gt_opr: Oid;
    let hashable: bool;

    /*
     * Look up the operators using the type cache.
     *
     * Note: the search algorithm used by typcache.c ensures that the results
     * are consistent, ie all from matching opclasses.
     */
    if !isHashable.is_null() {
        cache_flags =
            TYPECACHE_LT_OPR | TYPECACHE_EQ_OPR | TYPECACHE_GT_OPR | TYPECACHE_HASH_PROC;
    } else {
        cache_flags = TYPECACHE_LT_OPR | TYPECACHE_EQ_OPR | TYPECACHE_GT_OPR;
    }

    typentry = lookup_type_cache(argtype, cache_flags);
    lt_opr = (*typentry).lt_opr;
    eq_opr = (*typentry).eq_opr;
    gt_opr = (*typentry).gt_opr;
    hashable = OidIsValid((*typentry).hash_proc);

    /* Report errors if needed */
    if (needLT && !OidIsValid(lt_opr)) || (needGT && !OidIsValid(gt_opr)) {
        elog!(
            ERROR,
            "could not identify an ordering operator for type {}",
            CStr_to_str(format_type_be(argtype))
        );
        unreachable!();
    }
    if needEQ && !OidIsValid(eq_opr) {
        elog!(
            ERROR,
            "could not identify an equality operator for type {}",
            CStr_to_str(format_type_be(argtype))
        );
        unreachable!();
    }

    /* Return results as needed */
    if !ltOpr.is_null() {
        *ltOpr = lt_opr;
    }
    if !eqOpr.is_null() {
        *eqOpr = eq_opr;
    }
    if !gtOpr.is_null() {
        *gtOpr = gt_opr;
    }
    if !isHashable.is_null() {
        *isHashable = hashable;
    }
}

/* given operator tuple, return the operator OID */
pub unsafe fn oprid(op: Operator) -> Oid {
    (*(GETSTRUCT(op) as Form_pg_operator)).oid
}

/* given operator tuple, return the underlying function's OID */
pub unsafe fn oprfuncid(op: Operator) -> Oid {
    let pgopform: Form_pg_operator = GETSTRUCT(op) as Form_pg_operator;

    (*pgopform).oprcode
}

/* binary_oper_exact()
 * Check for an "exact" match to the specified operand types.
 *
 * If one operand is an unknown literal, assume it should be taken to be
 * the same type as the other operand for this purpose.  Also, consider
 * the possibility that the other operand is a domain type that needs to
 * be reduced to its base type to find an "exact" match.
 */
unsafe fn binary_oper_exact(opname: *mut List, mut arg1: Oid, mut arg2: Oid) -> Oid {
    let mut result: Oid;
    let mut was_unknown = false;

    /* Unspecified type for one of the arguments? then use the other */
    if (arg1 == UNKNOWNOID) && (arg2 != InvalidOid) {
        arg1 = arg2;
        was_unknown = true;
    } else if (arg2 == UNKNOWNOID) && (arg1 != InvalidOid) {
        arg2 = arg1;
        was_unknown = true;
    }

    result = OpernameGetOprid(opname, arg1, arg2);
    if OidIsValid(result) {
        return result;
    }

    if was_unknown {
        /* arg1 and arg2 are the same here, need only look at arg1 */
        let basetype: Oid = getBaseType(arg1);

        if basetype != arg1 {
            result = OpernameGetOprid(opname, basetype, basetype);
            if OidIsValid(result) {
                return result;
            }
        }
    }

    InvalidOid
}

/* oper_select_candidate()
 *		Given the input argtype array and one or more candidates
 *		for the operator, attempt to resolve the conflict.
 *
 * Returns FUNCDETAIL_NOTFOUND, FUNCDETAIL_MULTIPLE, or FUNCDETAIL_NORMAL.
 * In the success case the Oid of the best candidate is stored in *operOid.
 *
 * Note that the caller has already determined that there is no candidate
 * exactly matching the input argtype(s).  Incompatible candidates are not yet
 * pruned away, however.
 */
unsafe fn oper_select_candidate(
    nargs: c_int,
    input_typeids: *mut Oid,
    mut candidates: FuncCandidateList,
    operOid: *mut Oid, /* output argument */
) -> FuncDetailCode {
    let ncandidates: c_int;

    /*
     * Delete any candidates that cannot actually accept the given input
     * types, whether directly or by coercion.
     */
    ncandidates = func_match_argtypes(nargs, input_typeids, candidates, &mut candidates);

    /* Done if no candidate or only one candidate survives */
    if ncandidates == 0 {
        *operOid = InvalidOid;
        return FUNCDETAIL_NOTFOUND;
    }
    if ncandidates == 1 {
        *operOid = (*candidates).oid;
        return FUNCDETAIL_NORMAL;
    }

    /*
     * Use the same heuristics as for ambiguous functions to resolve the
     * conflict.
     */
    candidates = func_select_candidate(nargs, input_typeids, candidates);

    if !candidates.is_null() {
        *operOid = (*candidates).oid;
        return FUNCDETAIL_NORMAL;
    }

    *operOid = InvalidOid;
    FUNCDETAIL_MULTIPLE /* failed to select a best candidate */
}

/* oper() -- search for a binary operator
 * Given operator name, types of arg1 and arg2, return oper struct.
 *
 * IMPORTANT: the returned operator (if any) is only promised to be
 * coercion-compatible with the input datatypes.  Do not use this if
 * you need an exact- or binary-compatible match; see compatible_oper.
 *
 * If no matching operator found, return NULL if noError is true,
 * raise an error if it is false.  pstate and location are used only to report
 * the error position; pass NULL/-1 if not available.
 *
 * NOTE: on success, the returned object is a syscache entry.  The caller
 * must ReleaseSysCache() the entry when done with it.
 */
pub unsafe fn oper(
    pstate: *mut ParseState,
    opname: *mut List,
    mut ltypeId: Oid,
    mut rtypeId: Oid,
    noError: bool,
    location: c_int,
) -> Operator {
    let mut operOid: Oid;
    let mut key: OprCacheKey = std::mem::zeroed();
    let key_ok: bool;
    let mut fdresult: FuncDetailCode = FUNCDETAIL_NOTFOUND;
    let mut tup: HeapTuple = std::ptr::null_mut();

    /*
     * Try to find the mapping in the lookaside cache.
     */
    key_ok = make_oper_cache_key(pstate, &mut key, opname, ltypeId, rtypeId, location);

    if key_ok {
        operOid = find_oper_cache_entry(&mut key);
        if OidIsValid(operOid) {
            tup = SearchSysCache1(OPEROID, ObjectIdGetDatum(operOid));
            if HeapTupleIsValid(tup) {
                return tup as Operator;
            }
        }
    }

    /*
     * First try for an "exact" match.
     */
    operOid = binary_oper_exact(opname, ltypeId, rtypeId);
    if !OidIsValid(operOid) {
        /*
         * Otherwise, search for the most suitable candidate.
         */
        let clist: FuncCandidateList;

        /* Get binary operators of given name */
        clist = OpernameGetCandidates(opname, b'b' as c_char, false);

        /* No operators found? Then fail... */
        if !clist.is_null() {
            /*
             * Unspecified type for one of the arguments? then use the other
             * (XXX this is probably dead code?)
             */
            let mut inputOids: [Oid; 2] = [0; 2];

            if rtypeId == InvalidOid {
                rtypeId = ltypeId;
            } else if ltypeId == InvalidOid {
                ltypeId = rtypeId;
            }
            inputOids[0] = ltypeId;
            inputOids[1] = rtypeId;
            fdresult = oper_select_candidate(2, inputOids.as_mut_ptr(), clist, &mut operOid);
        }
    }

    if OidIsValid(operOid) {
        tup = SearchSysCache1(OPEROID, ObjectIdGetDatum(operOid));
    }

    if HeapTupleIsValid(tup) {
        if key_ok {
            make_oper_cache_entry(&mut key, operOid);
        }
    } else if !noError {
        op_error(pstate, opname, ltypeId, rtypeId, fdresult, location);
    }

    tup as Operator
}

/* compatible_oper()
 *	given an opname and input datatypes, find a compatible binary operator
 *
 *	This is tighter than oper() because it will not return an operator that
 *	requires coercion of the input datatypes (but binary-compatible operators
 *	are accepted).  Otherwise, the semantics are the same.
 */
pub unsafe fn compatible_oper(
    pstate: *mut ParseState,
    op: *mut List,
    arg1: Oid,
    arg2: Oid,
    noError: bool,
    location: c_int,
) -> Operator {
    let optup: Operator;
    let opform: Form_pg_operator;

    /* oper() will find the best available match */
    optup = oper(pstate, op, arg1, arg2, noError, location);
    if optup == std::ptr::null_mut() as Operator {
        return std::ptr::null_mut() as Operator; /* must be noError case */
    }

    /* but is it good enough? */
    opform = GETSTRUCT(optup) as Form_pg_operator;
    if IsBinaryCoercible(arg1, (*opform).oprleft) && IsBinaryCoercible(arg2, (*opform).oprright)
    {
        return optup;
    }

    /* nope... */
    ReleaseSysCache(optup);

    if !noError {
        elog!(
            ERROR,
            "operator requires run-time type coercion: {}",
            CStr_to_str(op_signature_string(op, arg1, arg2))
        );
        unreachable!();
    }

    std::ptr::null_mut() as Operator
}

/* compatible_oper_opid() -- get OID of a binary operator
 *
 * This is a convenience routine that extracts only the operator OID
 * from the result of compatible_oper().  InvalidOid is returned if the
 * lookup fails and noError is true.
 */
pub unsafe fn compatible_oper_opid(op: *mut List, arg1: Oid, arg2: Oid, noError: bool) -> Oid {
    let optup: Operator;
    let result: Oid;

    optup = compatible_oper(std::ptr::null_mut(), op, arg1, arg2, noError, -1);
    if !optup.is_null() {
        result = oprid(optup);
        ReleaseSysCache(optup);
        return result;
    }
    InvalidOid
}

/* left_oper() -- search for a unary left operator (prefix operator)
 * Given operator name and type of arg, return oper struct.
 *
 * IMPORTANT: the returned operator (if any) is only promised to be
 * coercion-compatible with the input datatype.  Do not use this if
 * you need an exact- or binary-compatible match.
 *
 * If no matching operator found, return NULL if noError is true,
 * raise an error if it is false.  pstate and location are used only to report
 * the error position; pass NULL/-1 if not available.
 *
 * NOTE: on success, the returned object is a syscache entry.  The caller
 * must ReleaseSysCache() the entry when done with it.
 */
pub unsafe fn left_oper(
    pstate: *mut ParseState,
    op: *mut List,
    arg: Oid,
    noError: bool,
    location: c_int,
) -> Operator {
    let mut operOid: Oid;
    let mut key: OprCacheKey = std::mem::zeroed();
    let key_ok: bool;
    let mut fdresult: FuncDetailCode = FUNCDETAIL_NOTFOUND;
    let mut tup: HeapTuple = std::ptr::null_mut();

    /*
     * Try to find the mapping in the lookaside cache.
     */
    key_ok = make_oper_cache_key(pstate, &mut key, op, InvalidOid, arg, location);

    if key_ok {
        operOid = find_oper_cache_entry(&mut key);
        if OidIsValid(operOid) {
            tup = SearchSysCache1(OPEROID, ObjectIdGetDatum(operOid));
            if HeapTupleIsValid(tup) {
                return tup as Operator;
            }
        }
    }

    /*
     * First try for an "exact" match.
     */
    operOid = OpernameGetOprid(op, InvalidOid, arg);
    if !OidIsValid(operOid) {
        /*
         * Otherwise, search for the most suitable candidate.
         */
        let clist: FuncCandidateList;

        /* Get prefix operators of given name */
        clist = OpernameGetCandidates(op, b'l' as c_char, false);

        /* No operators found? Then fail... */
        if !clist.is_null() {
            /*
             * The returned list has args in the form (0, oprright). Move the
             * useful data into args[0] to keep oper_select_candidate simple.
             * XXX we are assuming here that we may scribble on the list!
             */
            let mut clisti: FuncCandidateList;

            clisti = clist;
            while !clisti.is_null() {
                *(*clisti).args.as_mut_ptr().add(0) = *(*clisti).args.as_mut_ptr().add(1);
                clisti = (*clisti).next;
            }

            /*
             * We must run oper_select_candidate even if only one candidate,
             * otherwise we may falsely return a non-type-compatible operator.
             */
            let mut argvar = arg;
            fdresult = oper_select_candidate(1, &mut argvar, clist, &mut operOid);
        }
    }

    if OidIsValid(operOid) {
        tup = SearchSysCache1(OPEROID, ObjectIdGetDatum(operOid));
    }

    if HeapTupleIsValid(tup) {
        if key_ok {
            make_oper_cache_entry(&mut key, operOid);
        }
    } else if !noError {
        op_error(pstate, op, InvalidOid, arg, fdresult, location);
    }

    tup as Operator
}

/*
 * op_signature_string
 *		Build a string representing an operator name, including arg type(s).
 *		The result is something like "integer + integer".
 *
 * This is typically used in the construction of operator-not-found error
 * messages.
 */
pub unsafe fn op_signature_string(op: *mut List, arg1: Oid, arg2: Oid) -> *const c_char {
    let mut argbuf: StringInfoData = std::mem::zeroed();

    initStringInfo(&mut argbuf);

    if OidIsValid(arg1) {
        appendStringInfo(&mut argbuf, c"%s ".as_ptr(), format_type_be(arg1));
    }

    appendStringInfoString(&mut argbuf, NameListToString(op));

    appendStringInfo(&mut argbuf, c" %s".as_ptr(), format_type_be(arg2));

    argbuf.data /* return palloc'd string buffer */
}

/*
 * op_error - utility routine to complain about an unresolvable operator
 */
unsafe fn op_error(
    pstate: *mut ParseState,
    op: *mut List,
    arg1: Oid,
    arg2: Oid,
    fdresult: FuncDetailCode,
    location: c_int,
) {
    if fdresult == FUNCDETAIL_MULTIPLE {
        elog!(
            ERROR,
            "operator is not unique: {}",
            CStr_to_str(op_signature_string(op, arg1, arg2))
        );
        unreachable!();
    } else {
        elog!(
            ERROR,
            "operator does not exist: {}",
            CStr_to_str(op_signature_string(op, arg1, arg2))
        );
        unreachable!();
    }
}

/*
 * make_op()
 *		Operator expression construction.
 *
 * Transform operator expression ensuring type compatibility.
 * This is where some type conversion happens.
 *
 * last_srf should be a copy of pstate->p_last_srf from just before we
 * started transforming the operator's arguments; this is used for nested-SRF
 * detection.  If the caller will throw an error anyway for a set-returning
 * expression, it's okay to cheat and just pass pstate->p_last_srf.
 */
pub unsafe fn make_op(
    pstate: *mut ParseState,
    opname: *mut List,
    ltree: *mut Node,
    rtree: *mut Node,
    last_srf: *mut Node,
    location: c_int,
) -> *mut Expr {
    let ltypeId: Oid;
    let rtypeId: Oid;
    let tup: Operator;
    let opform: Form_pg_operator;
    let mut actual_arg_types: [Oid; 2] = [0; 2];
    let mut declared_arg_types: [Oid; 2] = [0; 2];
    let nargs: c_int;
    let args: *mut List;
    let rettype: Oid;
    let result: *mut OpExpr;

    /* Check it's not a postfix operator */
    if rtree.is_null() {
        ereport!(ERROR, "postfix operators are not supported");
        unreachable!();
    }

    /* Select the operator */
    if ltree.is_null() {
        /* prefix operator */
        rtypeId = exprType(rtree);
        ltypeId = InvalidOid;
        tup = left_oper(pstate, opname, rtypeId, false, location);
    } else {
        /* otherwise, binary operator */
        ltypeId = exprType(ltree);
        rtypeId = exprType(rtree);
        tup = oper(pstate, opname, ltypeId, rtypeId, false, location);
    }

    opform = GETSTRUCT(tup) as Form_pg_operator;

    /* Check it's not a shell */
    if !RegProcedureIsValid((*opform).oprcode) {
        elog!(
            ERROR,
            "operator is only a shell: {}",
            CStr_to_str(op_signature_string(
                opname,
                (*opform).oprleft,
                (*opform).oprright
            ))
        );
        unreachable!();
    }

    /* Do typecasting and build the expression tree */
    if ltree.is_null() {
        /* prefix operator */
        args = list_make1(rtree as *mut std::ffi::c_void);
        actual_arg_types[0] = rtypeId;
        declared_arg_types[0] = (*opform).oprright;
        nargs = 1;
    } else {
        /* otherwise, binary operator */
        args = list_make2(ltree as *mut std::ffi::c_void, rtree as *mut std::ffi::c_void);
        actual_arg_types[0] = ltypeId;
        actual_arg_types[1] = rtypeId;
        declared_arg_types[0] = (*opform).oprleft;
        declared_arg_types[1] = (*opform).oprright;
        nargs = 2;
    }

    /*
     * enforce consistency with polymorphic argument and return types,
     * possibly adjusting return type or declared_arg_types (which will be
     * used as the cast destination by make_fn_arguments)
     */
    rettype = enforce_generic_type_consistency(
        actual_arg_types.as_mut_ptr(),
        declared_arg_types.as_mut_ptr(),
        nargs,
        (*opform).oprresult,
        false,
    );

    /* perform the necessary typecasting of arguments */
    make_fn_arguments(
        pstate,
        args,
        actual_arg_types.as_mut_ptr(),
        declared_arg_types.as_mut_ptr(),
    );

    /* and build the expression node */
    result = makeNode!(OpExpr, T_OpExpr);
    (*result).opno = oprid(tup);
    (*result).opfuncid = (*opform).oprcode;
    (*result).opresulttype = rettype;
    (*result).opretset = get_func_retset((*opform).oprcode);
    /* opcollid and inputcollid will be set by parse_collate.c */
    (*result).args = args;
    (*result).location = location;

    /* if it returns a set, check that's OK */
    if (*result).opretset {
        check_srf_call_placement(pstate, last_srf, location);
        /* ... and remember it for error checks at higher levels */
        (*pstate).p_last_srf = result as *mut Node;
    }

    ReleaseSysCache(tup);

    result as *mut Expr
}

/*
 * make_scalar_array_op()
 *		Build expression tree for "scalar op ANY/ALL (array)" construct.
 */
pub unsafe fn make_scalar_array_op(
    pstate: *mut ParseState,
    opname: *mut List,
    useOr: bool,
    ltree: *mut Node,
    rtree: *mut Node,
    location: c_int,
) -> *mut Expr {
    let ltypeId: Oid;
    let rtypeId: Oid;
    let atypeId: Oid;
    let res_atypeId: Oid;
    let tup: Operator;
    let opform: Form_pg_operator;
    let mut actual_arg_types: [Oid; 2] = [0; 2];
    let mut declared_arg_types: [Oid; 2] = [0; 2];
    let args: *mut List;
    let rettype: Oid;
    let result: *mut ScalarArrayOpExpr;

    ltypeId = exprType(ltree);
    atypeId = exprType(rtree);

    /*
     * The right-hand input of the operator will be the element type of the
     * array.  However, if we currently have just an untyped literal on the
     * right, stay with that and hope we can resolve the operator.
     */
    if atypeId == UNKNOWNOID {
        rtypeId = UNKNOWNOID;
    } else {
        rtypeId = get_base_element_type(atypeId);
        if !OidIsValid(rtypeId) {
            ereport!(
                ERROR,
                "op ANY/ALL (array) requires array on right side"
            );
            unreachable!();
        }
    }

    /* Now resolve the operator */
    tup = oper(pstate, opname, ltypeId, rtypeId, false, location);
    opform = GETSTRUCT(tup) as Form_pg_operator;

    /* Check it's not a shell */
    if !RegProcedureIsValid((*opform).oprcode) {
        elog!(
            ERROR,
            "operator is only a shell: {}",
            CStr_to_str(op_signature_string(
                opname,
                (*opform).oprleft,
                (*opform).oprright
            ))
        );
        unreachable!();
    }

    args = list_make2(ltree as *mut std::ffi::c_void, rtree as *mut std::ffi::c_void);
    actual_arg_types[0] = ltypeId;
    actual_arg_types[1] = rtypeId;
    declared_arg_types[0] = (*opform).oprleft;
    declared_arg_types[1] = (*opform).oprright;

    /*
     * enforce consistency with polymorphic argument and return types,
     * possibly adjusting return type or declared_arg_types (which will be
     * used as the cast destination by make_fn_arguments)
     */
    rettype = enforce_generic_type_consistency(
        actual_arg_types.as_mut_ptr(),
        declared_arg_types.as_mut_ptr(),
        2,
        (*opform).oprresult,
        false,
    );

    /*
     * Check that operator result is boolean
     */
    if rettype != BOOLOID {
        ereport!(
            ERROR,
            "op ANY/ALL (array) requires operator to yield boolean"
        );
        unreachable!();
    }
    if get_func_retset((*opform).oprcode) {
        ereport!(
            ERROR,
            "op ANY/ALL (array) requires operator not to return a set"
        );
        unreachable!();
    }

    /*
     * Now switch back to the array type on the right, arranging for any
     * needed cast to be applied.  Beware of polymorphic operators here;
     * enforce_generic_type_consistency may or may not have replaced a
     * polymorphic type with a real one.
     */
    if IsPolymorphicType(declared_arg_types[1]) {
        /* assume the actual array type is OK */
        res_atypeId = atypeId;
    } else {
        res_atypeId = get_array_type(declared_arg_types[1]);
        if !OidIsValid(res_atypeId) {
            elog!(
                ERROR,
                "could not find array type for data type {}",
                CStr_to_str(format_type_be(declared_arg_types[1]))
            );
            unreachable!();
        }
    }
    actual_arg_types[1] = atypeId;
    declared_arg_types[1] = res_atypeId;

    /* perform the necessary typecasting of arguments */
    make_fn_arguments(
        pstate,
        args,
        actual_arg_types.as_mut_ptr(),
        declared_arg_types.as_mut_ptr(),
    );

    /* and build the expression node */
    result = makeNode!(ScalarArrayOpExpr, T_ScalarArrayOpExpr);
    (*result).opno = oprid(tup);
    (*result).opfuncid = (*opform).oprcode;
    (*result).hashfuncid = InvalidOid;
    (*result).negfuncid = InvalidOid;
    (*result).useOr = useOr;
    /* inputcollid will be set by parse_collate.c */
    (*result).args = args;
    (*result).location = location;

    ReleaseSysCache(tup);

    result as *mut Expr
}

/*
 * Lookaside cache to speed operator lookup.  Possibly this should be in
 * a separate module under utils/cache/ ?
 *
 * The idea here is that the mapping from operator name and given argument
 * types is constant for a given search path (or single specified schema OID)
 * so long as the contents of pg_operator and pg_cast don't change.  And that
 * mapping is pretty expensive to compute, especially for ambiguous operators;
 * this is mainly because there are a *lot* of instances of popular operator
 * names such as "=", and we have to check each one to see which is the
 * best match.  So once we have identified the correct mapping, we save it
 * in a cache that need only be flushed on pg_operator or pg_cast change.
 * (pg_cast must be considered because changes in the set of implicit casts
 * affect the set of applicable operators for any given input datatype.)
 *
 * XXX in principle, ALTER TABLE ... INHERIT could affect the mapping as
 * well, but we disregard that since there's no convenient way to find out
 * about it, and it seems a pretty far-fetched corner-case anyway.
 *
 * Note: at some point it might be worth doing a similar cache for function
 * lookups.  However, the potential gain is a lot less since (a) function
 * names are generally not overloaded as heavily as operator names, and
 * (b) we'd have to flush on pg_proc updates, which are probably a good
 * deal more common than pg_operator updates.
 */

/* The operator cache hashtable */
static mut OprCacheHash: *mut HTAB = std::ptr::null_mut();

/*
 * make_oper_cache_key
 *		Fill the lookup key struct given operator name and arg types.
 *
 * Returns true if successful, false if the search_path overflowed
 * (hence no caching is possible).
 *
 * pstate/location are used only to report the error position; pass NULL/-1
 * if not available.
 */
unsafe fn make_oper_cache_key(
    pstate: *mut ParseState,
    key: *mut OprCacheKey,
    opname: *mut List,
    ltypeId: Oid,
    rtypeId: Oid,
    location: c_int,
) -> bool {
    let mut schemaname: *mut c_char = std::ptr::null_mut();
    let mut opername: *mut c_char = std::ptr::null_mut();

    /* deconstruct the name list */
    DeconstructQualifiedName(opname, &mut schemaname, &mut opername);

    /* ensure zero-fill for stable hashing */
    MemSet(
        key as *mut std::ffi::c_void,
        0,
        std::mem::size_of::<OprCacheKey>(),
    );

    /* save operator name and input types into key */
    strlcpy((*key).oprname.as_mut_ptr(), opername, NAMEDATALEN);
    (*key).left_arg = ltypeId;
    (*key).right_arg = rtypeId;

    if !schemaname.is_null() {
        let mut pcbstate: ParseCallbackState = std::mem::zeroed();

        /* search only in exact schema given */
        setup_parser_errposition_callback(&mut pcbstate, pstate, location);
        (*key).search_path[0] = LookupExplicitNamespace(schemaname, false);
        cancel_parser_errposition_callback(&mut pcbstate);
    } else {
        /* get the active search path */
        if fetch_search_path_array((*key).search_path.as_mut_ptr(), MAX_CACHED_PATH_LEN as c_int)
            > MAX_CACHED_PATH_LEN as c_int
        {
            return false; /* oops, didn't fit */
        }
    }

    true
}

/*
 * find_oper_cache_entry
 *
 * Look for a cache entry matching the given key.  If found, return the
 * contained operator OID, else return InvalidOid.
 */
unsafe fn find_oper_cache_entry(key: *mut OprCacheKey) -> Oid {
    let oprentry: *mut OprCacheEntry;

    if OprCacheHash.is_null() {
        /* First time through: initialize the hash table */
        let mut ctl: HASHCTL = std::mem::zeroed();

        ctl.keysize = std::mem::size_of::<OprCacheKey>() as Size;
        ctl.entrysize = std::mem::size_of::<OprCacheEntry>() as Size;
        OprCacheHash = hash_create(
            c"Operator lookup cache".as_ptr(),
            256,
            &mut ctl,
            (HASH_ELEM | HASH_BLOBS) as c_int,
        );

        /* Arrange to flush cache on pg_operator and pg_cast changes */
        CacheRegisterSyscacheCallback(OPERNAMENSP, InvalidateOprCacheCallBack, 0 as Datum);
        CacheRegisterSyscacheCallback(CASTSOURCETARGET, InvalidateOprCacheCallBack, 0 as Datum);
    }

    /* Look for an existing entry */
    oprentry = hash_search(
        OprCacheHash,
        key as *mut std::ffi::c_void,
        HASH_FIND,
        std::ptr::null_mut(),
    ) as *mut OprCacheEntry;
    if oprentry.is_null() {
        return InvalidOid;
    }

    (*oprentry).opr_oid
}

/*
 * make_oper_cache_entry
 *
 * Insert a cache entry for the given key.
 */
unsafe fn make_oper_cache_entry(key: *mut OprCacheKey, opr_oid: Oid) {
    let oprentry: *mut OprCacheEntry;

    Assert!(!OprCacheHash.is_null());

    oprentry = hash_search(
        OprCacheHash,
        key as *mut std::ffi::c_void,
        HASH_ENTER,
        std::ptr::null_mut(),
    ) as *mut OprCacheEntry;
    (*oprentry).opr_oid = opr_oid;
}

/*
 * Callback for pg_operator and pg_cast inval events
 */
unsafe fn InvalidateOprCacheCallBack(_arg: Datum, _cacheid: c_int, _hashvalue: uint32) {
    let mut status: HASH_SEQ_STATUS = std::mem::zeroed();
    let mut hentry: *mut OprCacheEntry;

    Assert!(!OprCacheHash.is_null());

    /* Currently we just flush all entries; hard to be smarter ... */
    hash_seq_init(&mut status, OprCacheHash);

    loop {
        hentry = hash_seq_search(&mut status) as *mut OprCacheEntry;
        if hentry.is_null() {
            break;
        }
        if hash_search(
            OprCacheHash,
            &mut (*hentry).key as *mut OprCacheKey as *mut std::ffi::c_void,
            HASH_REMOVE,
            std::ptr::null_mut(),
        )
        .is_null()
        {
            elog!(ERROR, "hash table corrupted");
        }
    }
}

/* ------------------------------------------------------------------------
 * Local stubs for as-yet-unported dependencies.
 * ------------------------------------------------------------------------ */

/* parser/parse_oper.h: typedef HeapTuple Operator; */
pub type Operator = HeapTuple;

unsafe fn CStr_to_str<'a>(s: *const c_char) -> &'a str {
    if s.is_null() {
        return "";
    }
    std::ffi::CStr::from_ptr(s).to_str().unwrap_or("")
}

// HeapTuple, HeapTupleIsValid, GETSTRUCT imported from crate::access::htup_details

// TODO: catalog/pg_operator.h
pub type Form_pg_operator = *mut FormData_pg_operator;
#[repr(C)]
pub struct FormData_pg_operator {
    pub oid: Oid,
    pub oprcode: Oid,
    pub oprleft: Oid,
    pub oprright: Oid,
    pub oprresult: Oid,
}

// TODO: nodes/parsenodes.h
pub use crate::parser::parse_node::ParseState;
pub use crate::nodes::parsenodes::ObjectWithArgs;
pub type TypeName = std::ffi::c_void;
pub type Expr = std::ffi::c_void;
#[repr(C)]
pub struct OpExpr {
    pub opno: Oid,
    pub opfuncid: Oid,
    pub opresulttype: Oid,
    pub opretset: bool,
    pub args: *mut List,
    pub location: c_int,
}
#[repr(C)]
pub struct ScalarArrayOpExpr {
    pub opno: Oid,
    pub opfuncid: Oid,
    pub hashfuncid: Oid,
    pub negfuncid: Oid,
    pub useOr: bool,
    pub args: *mut List,
    pub location: c_int,
}

// TODO: parser/parse_node.h
pub type ParseCallbackState = std::ffi::c_void;

// TODO: utils/typcache.h
#[repr(C)]
pub struct TypeCacheEntry {
    pub lt_opr: Oid,
    pub eq_opr: Oid,
    pub gt_opr: Oid,
    pub hash_proc: Oid,
}
pub const TYPECACHE_LT_OPR: c_int = 0;
pub const TYPECACHE_EQ_OPR: c_int = 0;
pub const TYPECACHE_GT_OPR: c_int = 0;
pub const TYPECACHE_HASH_PROC: c_int = 0;
unsafe fn lookup_type_cache(_type_id: Oid, _flags: c_int) -> *mut TypeCacheEntry {
    unimplemented!() // TODO: utils/typcache.c
}

// TODO: parser/parse_func.h
pub type FuncDetailCode = c_int;
pub const FUNCDETAIL_NOTFOUND: FuncDetailCode = 0;
pub const FUNCDETAIL_MULTIPLE: FuncDetailCode = 1;
pub const FUNCDETAIL_NORMAL: FuncDetailCode = 2;

// TODO: catalog/namespace.h (FuncCandidateList)
#[repr(C)]
pub struct _FuncCandidateList {
    pub next: *mut _FuncCandidateList,
    pub oid: Oid,
    pub args: [Oid; 1], /* FLEXIBLE_ARRAY_MEMBER */
}
pub type FuncCandidateList = *mut _FuncCandidateList;

unsafe fn OpernameGetOprid(_names: *mut List, _oprleft: Oid, _oprright: Oid) -> Oid {
    unimplemented!() // TODO: catalog/namespace.c
}
unsafe fn OpernameGetCandidates(
    _names: *mut List,
    _oprkind: c_char,
    _missing_schema_ok: bool,
) -> FuncCandidateList {
    unimplemented!() // TODO: catalog/namespace.c
}
unsafe fn LookupExplicitNamespace(_nspname: *const c_char, _missing_ok: bool) -> Oid {
    unimplemented!() // TODO: catalog/namespace.c
}
unsafe fn fetch_search_path_array(_sarray: *mut Oid, _sarray_len: c_int) -> c_int {
    unimplemented!() // TODO: catalog/namespace.c
}
unsafe fn DeconstructQualifiedName(
    _names: *mut List,
    _nspname_p: *mut *mut c_char,
    _objname_p: *mut *mut c_char,
) {
    unimplemented!() // TODO: catalog/namespace.c
}

unsafe fn func_match_argtypes(
    _nargs: c_int,
    _input_typeids: *mut Oid,
    _raw_candidates: FuncCandidateList,
    _candidates: *mut FuncCandidateList,
) -> c_int {
    unimplemented!() // TODO: parser/parse_func.c
}
unsafe fn func_select_candidate(
    _nargs: c_int,
    _input_typeids: *mut Oid,
    _candidates: FuncCandidateList,
) -> FuncCandidateList {
    unimplemented!() // TODO: parser/parse_func.c
}
unsafe fn make_fn_arguments(
    _pstate: *mut ParseState,
    _fargs: *mut List,
    _actual_arg_types: *mut Oid,
    _declared_arg_types: *mut Oid,
) {
    unimplemented!() // TODO: parser/parse_func.c
}

unsafe fn LookupTypeNameOid(
    _pstate: *mut ParseState,
    _typeName: *mut TypeName,
    _missing_ok: bool,
) -> Oid {
    unimplemented!() // TODO: parser/parse_type.c
}

// TODO: parser/parse_coerce.h
unsafe fn IsBinaryCoercible(_srctype: Oid, _targettype: Oid) -> bool {
    unimplemented!() // TODO: parser/parse_coerce.c
}
unsafe fn enforce_generic_type_consistency(
    _actual_arg_types: *mut Oid,
    _declared_arg_types: *mut Oid,
    _nargs: c_int,
    _rettype: Oid,
    _allow_poly: bool,
) -> Oid {
    unimplemented!() // TODO: parser/parse_coerce.c
}

// TODO: nodes/nodeFuncs.h
unsafe fn exprType(_expr: *const Node) -> Oid {
    unimplemented!() // TODO: nodes/nodeFuncs.c
}

// TODO: utils/syscache.h
pub const OPEROID: c_int = 0;
unsafe fn SearchSysCache1(_cacheId: c_int, _key1: Datum) -> HeapTuple {
    unimplemented!() // TODO: utils/cache/syscache.c
}
unsafe fn ReleaseSysCache(_tuple: HeapTuple) {
    unimplemented!() // TODO: utils/cache/syscache.c
}

// TODO: utils/inval.h
pub const OPERNAMENSP: c_int = 0;
pub const CASTSOURCETARGET: c_int = 0;
unsafe fn CacheRegisterSyscacheCallback(
    _cacheid: c_int,
    _func: unsafe fn(Datum, c_int, uint32),
    _arg: Datum,
) {
    unimplemented!() // TODO: utils/cache/inval.c
}

// TODO: utils/lsyscache.h
unsafe fn getBaseType(_typid: Oid) -> Oid {
    unimplemented!() // TODO: utils/cache/lsyscache.c
}
unsafe fn get_base_element_type(_typid: Oid) -> Oid {
    unimplemented!() // TODO: utils/cache/lsyscache.c
}
unsafe fn get_array_type(_typid: Oid) -> Oid {
    unimplemented!() // TODO: utils/cache/lsyscache.c
}
unsafe fn get_func_retset(_funcid: Oid) -> bool {
    unimplemented!() // TODO: utils/cache/lsyscache.c
}

// TODO: utils/builtins.h
unsafe fn format_type_be(_type_oid: Oid) -> *const c_char {
    unimplemented!() // TODO: utils/adt/format_type.c
}

// TODO: nodes/makefuncs.h / list helpers
unsafe fn list_length(_l: *const List) -> c_int {
    unimplemented!() // TODO: nodes/list.c
}
unsafe fn list_make1(_d1: *mut std::ffi::c_void) -> *mut List {
    unimplemented!() // TODO: nodes/list.c
}
unsafe fn list_make2(_d1: *mut std::ffi::c_void, _d2: *mut std::ffi::c_void) -> *mut List {
    unimplemented!() // TODO: nodes/list.c
}
unsafe fn NameListToString(_names: *mut List) -> *const c_char {
    unimplemented!() // TODO: catalog/namespace.c
}

// TODO: lib/stringinfo.h
#[repr(C)]
pub struct StringInfoData {
    pub data: *mut c_char,
    pub len: c_int,
    pub maxlen: c_int,
    pub cursor: c_int,
}
unsafe fn initStringInfo(_str: *mut StringInfoData) {
    unimplemented!() // TODO: lib/stringinfo.c
}
extern "C" {
    fn appendStringInfo(str: *mut StringInfoData, fmt: *const c_char, ...);
}
unsafe fn appendStringInfoString(_str: *mut StringInfoData, _s: *const c_char) {
    unimplemented!() // TODO: lib/stringinfo.c
}

// TODO: utils/hsearch.h
pub type HTAB = std::ffi::c_void;
#[repr(C)]
pub struct HASHCTL {
    pub keysize: Size,
    pub entrysize: Size,
}
#[repr(C)]
pub struct HASH_SEQ_STATUS {
    pub hashp: *mut HTAB,
    pub curBucket: u32,
    pub curEntry: *mut std::ffi::c_void,
}
pub const HASH_ELEM: c_int = 0x0008;
pub const HASH_BLOBS: c_int = 0x0010;
pub const HASH_FIND: c_int = 0;
pub const HASH_ENTER: c_int = 1;
pub const HASH_REMOVE: c_int = 2;
unsafe fn hash_create(
    _tabname: *const c_char,
    _nelem: c_long_t,
    _info: *mut HASHCTL,
    _flags: c_int,
) -> *mut HTAB {
    unimplemented!() // TODO: utils/hash/dynahash.c
}
unsafe fn hash_search(
    _hashp: *mut HTAB,
    _key_ptr: *mut std::ffi::c_void,
    _action: c_int,
    _found_ptr: *mut bool,
) -> *mut std::ffi::c_void {
    unimplemented!() // TODO: utils/hash/dynahash.c
}
unsafe fn hash_seq_init(_status: *mut HASH_SEQ_STATUS, _hashp: *mut HTAB) {
    unimplemented!() // TODO: utils/hash/dynahash.c
}
unsafe fn hash_seq_search(_status: *mut HASH_SEQ_STATUS) -> *mut std::ffi::c_void {
    unimplemented!() // TODO: utils/hash/dynahash.c
}
type c_long_t = std::ffi::c_long;

// TODO: parser/parsenodes.h types (Oid constants)
pub const UNKNOWNOID: Oid = 705;
pub const BOOLOID: Oid = 16;

unsafe fn IsPolymorphicType(_typid: Oid) -> bool {
    unimplemented!() // TODO: catalog/pg_type.h
}
unsafe fn RegProcedureIsValid(p: Oid) -> bool {
    OidIsValid(p)
}

// TODO: parser/parse_node.h callbacks
unsafe fn setup_parser_errposition_callback(
    _pcbstate: *mut ParseCallbackState,
    _pstate: *mut ParseState,
    _location: c_int,
) {
    unimplemented!() // TODO: parser/parse_node.c
}
unsafe fn cancel_parser_errposition_callback(_pcbstate: *mut ParseCallbackState) {
    unimplemented!() // TODO: parser/parse_node.c
}

// TODO: parser/parse_func.h
unsafe fn check_srf_call_placement(
    _pstate: *mut ParseState,
    _last_srf: *mut Node,
    _location: c_int,
) {
    unimplemented!() // TODO: parser/parse_func.c
}

unsafe fn strlcpy(_dst: *mut c_char, _src: *const c_char, _siz: usize) -> usize {
    unimplemented!() // TODO: port/strlcpy.c
}
// MemSet provided by crate::c (re-exported via crate::prelude::*)
