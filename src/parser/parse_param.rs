//! parse_param.c - handle $n parameter references in the parser.
//!
//! Covers the two core-backend cases: a fixed list of parameters with known
//! types, and an expandable list whose types may be deduced from context.
//! Only explicit $n references (ParamRef nodes) are supported here.

use crate::prelude::*;
use crate::{IsA, foreach, current_cell, makeNode, DirectFunctionCall1};

use crate::nodes::nodes::{Node, NodeTag};
use crate::nodes::parsenodes::{ParamRef, Query};
use crate::nodes::primnodes::{Param, ParamKind, PARAM_EXTERN};
use crate::nodes::nodeFuncs::{
    expression_tree_walker, query_tree_walker, tree_walker_callback,
};
use crate::catalog::pg_type_d::{UNKNOWNOID, VOIDOID};
use crate::utils::builtins::format_type_be;

/// `ParseExprKind` (parser/parse_node.h) - not yet ported. We only need the
/// CALL-argument discriminant for the JDBC procedure-call hack below.
///
/// TODO(pg-port): replace with the real ParseExprKind once parse_node.h lands.
pub type ParseExprKind = c_int;
/// `EXPR_KIND_CALL_ARGUMENT` - argument of a CALL statement.
/// TODO(pg-port): keep in sync with the real enum value when parse_node.h ports.
pub const EXPR_KIND_CALL_ARGUMENT: ParseExprKind = 41;

/// Hook function pointer types from parse_node.h.
/// TODO(pg-port): move these to the real ParseState once parse_node.h is ported.
pub type PreParseColumnRefHook = ();
pub type PostParseColumnRefHook = ();
pub type ParseParamRefHook =
    Option<unsafe fn(pstate: *mut ParseState, pref: *mut ParamRef) -> *mut Node>;
pub type CoerceParamHook = Option<
    unsafe fn(
        pstate: *mut ParseState,
        param: *mut Param,
        targetTypeId: Oid,
        targetTypeMod: int32,
        location: c_int,
    ) -> *mut Node,
>;

/// Local stub of `ParseState` (parser/parse_node.h) with the fields this module
/// needs. The full struct is large; parse_param.c only ever reaches the param
/// hook state, the param/coerce hooks, and `p_expr_kind`.
///
/// TODO(pg-port): replace with the real ParseState once parse_node.h is ported.
/// `parse_enr.rs` carries its own (smaller) stub of the same struct; both should
/// collapse onto the real definition when it lands.
pub struct ParseState {
    /// `void *p_ref_hook_state` - common passthrough link for the ref hooks.
    pub p_ref_hook_state: *mut c_void,
    /// `ParseParamRefHook p_paramref_hook`.
    pub p_paramref_hook: ParseParamRefHook,
    /// `CoerceParamHook p_coerce_param_hook`.
    pub p_coerce_param_hook: CoerceParamHook,
    /// `ParseExprKind p_expr_kind` - expression kind under analysis.
    pub p_expr_kind: ParseExprKind,
}

/// `FixedParamState` - a fixed array of parameter type OIDs.
#[repr(C)]
struct FixedParamState {
    /// array of parameter type OIDs.
    paramTypes: *const Oid,
    /// number of array entries.
    numParams: c_int,
}

/// `VarParamState` - an expandable caller-supplied OID array. A zero entry means
/// the parameter number hasn't been seen; UNKNOWNOID means it's been used but
/// its type is not yet known.
#[repr(C)]
struct VarParamState {
    /// pointer to the caller's array of parameter type OIDs.
    paramTypes: *mut *mut Oid,
    /// pointer to the caller's count of array entries.
    numParams: *mut c_int,
}

// ---- locally-stubbed, not-yet-ported callees -------------------------------

/// `get_typcollation` (utils/cache/lsyscache.c) - default collation of a type.
/// TODO(pg-port): use the real lsyscache routine once ported.
unsafe fn get_typcollation(_typid: Oid) -> Oid {
    crate::utils::cache::lsyscache::get_typcollation(_typid)
}

/// `parser_errposition` (parser/parse_node.c) - report an error cursor position.
/// In the real backend this contributes an errposition to the in-flight
/// ereport; here it is a no-op placeholder.
/// TODO(pg-port): wire to the real parse_node.c routine once ported.
unsafe fn parser_errposition(_pstate: *mut ParseState, _location: c_int) -> c_int {
    crate::parser::parse_node::parser_errposition(_pstate as _, _location)
}

/*
 * Set up to process a query containing references to fixed parameters.
 */
pub unsafe fn setup_parse_fixed_parameters(
    pstate: *mut ParseState,
    paramTypes: *const Oid,
    numParams: c_int,
) {
    let parstate = palloc(core::mem::size_of::<FixedParamState>()) as *mut FixedParamState;

    (*parstate).paramTypes = paramTypes;
    (*parstate).numParams = numParams;
    (*pstate).p_ref_hook_state = parstate as *mut c_void;
    (*pstate).p_paramref_hook = Some(fixed_paramref_hook);
    /* no need to use p_coerce_param_hook */
}

/*
 * Set up to process a query containing references to variable parameters.
 */
pub unsafe fn setup_parse_variable_parameters(
    pstate: *mut ParseState,
    paramTypes: *mut *mut Oid,
    numParams: *mut c_int,
) {
    let parstate = palloc(core::mem::size_of::<VarParamState>()) as *mut VarParamState;

    (*parstate).paramTypes = paramTypes;
    (*parstate).numParams = numParams;
    (*pstate).p_ref_hook_state = parstate as *mut c_void;
    (*pstate).p_paramref_hook = Some(variable_paramref_hook);
    (*pstate).p_coerce_param_hook = Some(variable_coerce_param_hook);
}

/*
 * Transform a ParamRef using fixed parameter types.
 */
unsafe fn fixed_paramref_hook(pstate: *mut ParseState, pref: *mut ParamRef) -> *mut Node {
    let parstate = (*pstate).p_ref_hook_state as *mut FixedParamState;
    let paramno = (*pref).number;

    /* Check parameter number is valid */
    if paramno <= 0
        || paramno > (*parstate).numParams
        || !OidIsValid(*(*parstate).paramTypes.offset((paramno - 1) as isize))
    {
        let _ = parser_errposition(pstate, (*pref).location);
        elog!(ERROR, "there is no parameter ${}", paramno);
    }

    let param = makeNode!(Param, T_Param);
    (*param).paramkind = PARAM_EXTERN;
    (*param).paramid = paramno;
    (*param).paramtype = *(*parstate).paramTypes.offset((paramno - 1) as isize);
    (*param).paramtypmod = -1;
    (*param).paramcollid = get_typcollation((*param).paramtype);
    (*param).location = (*pref).location;

    param as *mut Node
}

/*
 * Transform a ParamRef using variable parameter types.
 *
 * The only difference here is we must enlarge the parameter type array
 * as needed.
 */
unsafe fn variable_paramref_hook(pstate: *mut ParseState, pref: *mut ParamRef) -> *mut Node {
    let parstate = (*pstate).p_ref_hook_state as *mut VarParamState;
    let paramno = (*pref).number;

    /* Check parameter number is in range */
    if paramno <= 0
        || (paramno as Size) > MaxAllocSize / core::mem::size_of::<Oid>()
    {
        let _ = parser_errposition(pstate, (*pref).location);
        elog!(ERROR, "there is no parameter ${}", paramno);
    }
    if paramno > *(*parstate).numParams {
        /* Need to enlarge param array */
        if !(*(*parstate).paramTypes).is_null() {
            *(*parstate).paramTypes = repalloc0_array_oid(
                *(*parstate).paramTypes,
                *(*parstate).numParams,
                paramno,
            );
        } else {
            *(*parstate).paramTypes = palloc0_array_oid(paramno);
        }
        *(*parstate).numParams = paramno;
    }

    /* Locate param's slot in array */
    let pptype = (*(*parstate).paramTypes).offset((paramno - 1) as isize);

    /* If not seen before, initialize to UNKNOWN type */
    if *pptype == InvalidOid {
        *pptype = UNKNOWNOID;
    }

    /*
     * If the argument is of type void and it's procedure call, interpret it
     * as unknown.  This allows the JDBC driver to not have to distinguish
     * function and procedure calls.  See also another component of this hack
     * in ParseFuncOrColumn().
     */
    if *pptype == VOIDOID && (*pstate).p_expr_kind == EXPR_KIND_CALL_ARGUMENT {
        *pptype = UNKNOWNOID;
    }

    let param = makeNode!(Param, T_Param);
    (*param).paramkind = PARAM_EXTERN;
    (*param).paramid = paramno;
    (*param).paramtype = *pptype;
    (*param).paramtypmod = -1;
    (*param).paramcollid = get_typcollation((*param).paramtype);
    (*param).location = (*pref).location;

    param as *mut Node
}

/*
 * Coerce a Param to a query-requested datatype, in the varparams case.
 */
unsafe fn variable_coerce_param_hook(
    pstate: *mut ParseState,
    param: *mut Param,
    targetTypeId: Oid,
    _targetTypeMod: int32,
    location: c_int,
) -> *mut Node {
    if (*param).paramkind == PARAM_EXTERN && (*param).paramtype == UNKNOWNOID {
        /*
         * Input is a Param of previously undetermined type, and we want to
         * update our knowledge of the Param's type.
         */
        let parstate = (*pstate).p_ref_hook_state as *mut VarParamState;
        let paramTypes = *(*parstate).paramTypes;
        let paramno = (*param).paramid;

        if paramno <= 0 /* shouldn't happen, but... */
            || paramno > *(*parstate).numParams
        {
            let _ = parser_errposition(pstate, (*param).location);
            elog!(ERROR, "there is no parameter ${}", paramno);
        }

        if *paramTypes.offset((paramno - 1) as isize) == UNKNOWNOID {
            /* We've successfully resolved the type */
            *paramTypes.offset((paramno - 1) as isize) = targetTypeId;
        } else if *paramTypes.offset((paramno - 1) as isize) == targetTypeId {
            /* We previously resolved the type, and it matches */
        } else {
            /* Oops */
            let prev = *paramTypes.offset((paramno - 1) as isize);
            let _ = parser_errposition(pstate, (*param).location);
            elog!(
                ERROR,
                "inconsistent types deduced for parameter ${}: {:?} versus {:?}",
                paramno,
                format_type_be(prev),
                format_type_be(targetTypeId)
            );
        }

        (*param).paramtype = targetTypeId;

        /*
         * Note: it is tempting here to set the Param's paramtypmod to
         * targetTypeMod, but that is probably unwise because we have no
         * infrastructure that enforces that the value delivered for a Param
         * will match any particular typmod.  Leaving it -1 ensures that a
         * run-time length check/coercion will occur if needed.
         */
        (*param).paramtypmod = -1;

        /*
         * This module always sets a Param's collation to be the default for
         * its datatype.  If that's not what you want, you should be using the
         * more general parser substitution hooks.
         */
        (*param).paramcollid = get_typcollation((*param).paramtype);

        /* Use the leftmost of the param's and coercion's locations */
        if location >= 0 && ((*param).location < 0 || location < (*param).location) {
            (*param).location = location;
        }

        return param as *mut Node;
    }

    /* Else signal to proceed with normal coercion */
    null_mut()
}

/*
 * Check for consistent assignment of variable parameters after completion
 * of parsing with parse_variable_parameters.
 *
 * Note: this code intentionally does not check that all parameter positions
 * were used, nor that all got non-UNKNOWN types assigned.  Caller of parser
 * should enforce that if it's important.
 */
pub unsafe fn check_variable_parameters(pstate: *mut ParseState, query: *mut Query) {
    let parstate = (*pstate).p_ref_hook_state as *mut VarParamState;

    /* If numParams is zero then no Params were generated, so no work */
    if *(*parstate).numParams > 0 {
        let _ = query_tree_walker(
            query,
            Some(check_parameter_resolution_walker),
            pstate as *mut c_void,
            0,
        );
    }
}

/*
 * Traverse a fully-analyzed tree to verify that parameter symbols
 * match their types.  We need this because some Params might still
 * be UNKNOWN, if there wasn't anything to force their coercion,
 * and yet other instances seen later might have gotten coerced.
 */
unsafe fn check_parameter_resolution_walker(node: *mut Node, pstate: *mut c_void) -> bool {
    let pstate = pstate as *mut ParseState;
    if node.is_null() {
        return false;
    }
    if IsA!(node, T_Param) {
        let param = node as *mut Param;

        if (*param).paramkind == PARAM_EXTERN {
            let parstate = (*pstate).p_ref_hook_state as *mut VarParamState;
            let paramno = (*param).paramid;

            if paramno <= 0 /* shouldn't happen, but... */
                || paramno > *(*parstate).numParams
            {
                let _ = parser_errposition(pstate, (*param).location);
                elog!(ERROR, "there is no parameter ${}", paramno);
            }

            if (*param).paramtype != *(*(*parstate).paramTypes).offset((paramno - 1) as isize) {
                let _ = parser_errposition(pstate, (*param).location);
                elog!(
                    ERROR,
                    "could not determine data type of parameter ${}",
                    paramno
                );
            }
        }
        return false;
    }
    if IsA!(node, T_Query) {
        /* Recurse into RTE subquery or not-yet-planned sublink subquery */
        return query_tree_walker(
            node as *mut Query,
            Some(check_parameter_resolution_walker),
            pstate as *mut c_void,
            0,
        );
    }
    expression_tree_walker(
        node,
        Some(check_parameter_resolution_walker),
        pstate as *mut c_void,
    )
}

/*
 * Check to see if a fully-parsed query tree contains any PARAM_EXTERN Params.
 */
pub unsafe fn query_contains_extern_params(query: *mut Query) -> bool {
    query_tree_walker(
        query,
        Some(query_contains_extern_params_walker),
        null_mut(),
        0,
    )
}

unsafe fn query_contains_extern_params_walker(node: *mut Node, context: *mut c_void) -> bool {
    if node.is_null() {
        return false;
    }
    if IsA!(node, T_Param) {
        let param = node as *mut Param;

        if (*param).paramkind == PARAM_EXTERN {
            return true;
        }
        return false;
    }
    if IsA!(node, T_Query) {
        /* Recurse into RTE subquery or not-yet-planned sublink subquery */
        return query_tree_walker(
            node as *mut Query,
            Some(query_contains_extern_params_walker),
            context,
            0,
        );
    }
    expression_tree_walker(
        node,
        Some(query_contains_extern_params_walker),
        context,
    )
}

// ---- local alloc helpers (palloc0_array / repalloc0_array for Oid) ---------

/// `palloc0_array(Oid, n)`: zeroed array of `n` Oids.
unsafe fn palloc0_array_oid(n: c_int) -> *mut Oid {
    palloc0((n as Size) * core::mem::size_of::<Oid>()) as *mut Oid
}

/// `repalloc0_array(arr, Oid, oldlen, newlen)`: grow `arr` to `newlen` entries,
/// zeroing the newly-added tail.
unsafe fn repalloc0_array_oid(arr: *mut Oid, oldlen: c_int, newlen: c_int) -> *mut Oid {
    repalloc0(
        arr as *mut c_void,
        (oldlen as Size) * core::mem::size_of::<Oid>(),
        (newlen as Size) * core::mem::size_of::<Oid>(),
    ) as *mut Oid
}

// Suppress unused-type warnings for the hook-typedef placeholders that exist
// only to mirror parse_node.h field types until that header is ported.
const _: fn() = || {
    let _: PreParseColumnRefHook;
    let _: PostParseColumnRefHook;
};
