//! src/backend/utils/adt/arraysubs.c
//!
//! Subscripting support functions for arrays.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void};

use crate::c::{int16, int32};
use crate::postgres_ext::Oid;
use crate::postgres::{DatumGetInt32, Int32GetDatum, PointerGetDatum};

use crate::nodes::nodes::{Node, NodeTag};
use crate::nodes::nodes::NodeTag::{T_A_Indices, T_Param, T_SupportRequestModifyInPlace};
use crate::nodes::pg_list::{lappend, linitial, list_length, lsecond, List, NIL};
use crate::nodes::parsenodes::A_Indices;
use crate::nodes::primnodes::{Const, Param, SubscriptingRef};
use crate::nodes::primnodes::CoercionContext::{self, COERCION_ASSIGNMENT};
use crate::nodes::primnodes::CoercionForm::{self, COERCE_IMPLICIT_CAST};
use crate::nodes::primnodes::ParamKind::PARAM_EXTERN;
use crate::nodes::execnodes::{ExprContext, ExprEvalStep, ExprState};
use crate::nodes::subscripting::SubscriptRoutines;
use crate::nodes::supportnodes::SupportRequestModifyInPlace;
use crate::parser::parse_node::ParseState;
use crate::utils::array::ArrayType;
use crate::catalog::pg_type_d::INT4OID;
use crate::postgres_ext::InvalidOid;
use crate::utils::elog::ERROR;

use crate::{foreach, current_cell, lfirst_node, IsA, ereport, elog};
use crate::{PG_RETURN_POINTER, PG_GETARG_POINTER};

// ----------------------------------------------------------------------------
// Local definitions for symbols with no usable definition elsewhere.
//
// MAXDIM is the implementation limit on number of array subscripts; defined in
// src/include/utils/array.h.  Not yet ported, so define locally.
// ----------------------------------------------------------------------------
const MAXDIM: c_int = 6;

/*
 * SubscriptingRefState and SubscriptExecSteps are declared as opaque c_void
 * aliases in nodes/subscripting.rs, which does not permit the field access this
 * file requires.  Define faithful local structs here, matching execExpr.h /
 * execnodes.h, until those headers are properly ported.
 */
#[repr(C)]
pub struct SubscriptingRefState {
    pub isassignment: bool,            /* is it assignment, or just fetch? */
    pub refelemtype: Oid,              /* OID of the container element type */
    pub refattrlength: int16,          /* typlen of container type */
    pub refelemlength: int16,          /* typlen of the container element type */
    pub refelembyval: bool,            /* is the element type pass-by-value? */
    pub refelemalign: c_char,          /* typalign of the element type */

    /* numupper and upperprovided[] are filled at compile time */
    pub numupper: c_int,
    pub upperprovided: *mut bool,      /* indicates if this position is supplied */
    pub upperindex: *mut Datum,
    pub upperindexnull: *mut bool,

    /* similarly for lower indexes, if any */
    pub numlower: c_int,
    pub lowerprovided: *mut bool,
    pub lowerindex: *mut Datum,
    pub lowerindexnull: *mut bool,

    /* for assignment, new value to assign is evaluated into here */
    pub replacevalue: Datum,
    pub replacenull: bool,

    /* if we have a nested assignment, SBSREF_OLD value is here */
    pub prevvalue: Datum,
    pub prevnull: bool,

    /* workspace for the subscripting type's functions */
    pub workspace: *mut c_void,
}

pub type ExecEvalSubroutine = Option<
    unsafe fn(state: *mut ExprState, op: *mut ExprEvalStep, econtext: *mut ExprContext),
>;

pub type ExecEvalBoolSubroutine = Option<
    unsafe fn(state: *mut ExprState, op: *mut ExprEvalStep, econtext: *mut ExprContext) -> bool,
>;

#[repr(C)]
pub struct SubscriptExecSteps {
    /* See nodeSubscript.c comments for these */
    pub sbs_check_subscripts: ExecEvalBoolSubroutine,
    pub sbs_fetch: ExecEvalSubroutine,
    pub sbs_assign: ExecEvalSubroutine,
    pub sbs_fetch_old: ExecEvalSubroutine,
}

/* SubscriptingRefState.workspace for array subscripting execution */
#[repr(C)]
pub struct ArraySubWorkspace {
    /* Values determined during expression compilation */
    pub refelemtype: Oid,       /* OID of the array element type */
    pub refattrlength: int16,   /* typlen of array type */
    pub refelemlength: int16,   /* typlen of the array element type */
    pub refelembyval: bool,     /* is the element type pass-by-value? */
    pub refelemalign: c_char,   /* typalign of the element type */

    /*
     * Subscript values converted to integers.  Note that these arrays must be
     * of length MAXDIM even when dealing with fewer subscripts, because
     * array_get/set_slice may scribble on the extra entries.
     */
    pub upperindex: [c_int; MAXDIM as usize],
    pub lowerindex: [c_int; MAXDIM as usize],
}

/*
 * Finish parse analysis of a SubscriptingRef expression for an array.
 *
 * Transform the subscript expressions, coerce them to integers,
 * and determine the result type of the SubscriptingRef node.
 */
unsafe fn array_subscript_transform(
    sbsref: *mut SubscriptingRef,
    indirection: *mut List,
    pstate: *mut ParseState,
    isSlice: bool,
    isAssignment: bool,
) {
    let mut upperIndexpr: *mut List = NIL;
    let mut lowerIndexpr: *mut List = NIL;

    /*
     * Transform the subscript expressions, and separate upper and lower
     * bounds into two lists.
     *
     * If we have a container slice expression, we convert any non-slice
     * indirection items to slices by treating the single subscript as the
     * upper bound and supplying an assumed lower bound of 1.
     */
    foreach!(idx, indirection, {
        let ai = lfirst_node!(A_Indices, T_A_Indices, current_cell!(idx));
        let mut subexpr: *mut Node;

        if isSlice {
            if !(*ai).lidx.is_null() {
                subexpr = transformExpr(pstate, (*ai).lidx, (*pstate).p_expr_kind);
                /* If it's not int4 already, try to coerce */
                subexpr = coerce_to_target_type(
                    pstate,
                    subexpr,
                    exprType(subexpr),
                    INT4OID,
                    -1,
                    COERCION_ASSIGNMENT,
                    COERCE_IMPLICIT_CAST,
                    -1,
                );
                if subexpr.is_null() {
                    ereport!(
                        ERROR,
                        "array subscript must have type integer"
                    );
                }
            } else if !(*ai).is_slice {
                /* Make a constant 1 */
                subexpr = makeConst(
                    INT4OID,
                    -1,
                    InvalidOid,
                    std::mem::size_of::<int32>() as c_int,
                    Int32GetDatum(1),
                    false,
                    true,
                ) as *mut Node; /* pass by value */
            } else {
                /* Slice with omitted lower bound, put NULL into the list */
                subexpr = std::ptr::null_mut();
            }
            lowerIndexpr = lappend(lowerIndexpr, subexpr as *mut std::ffi::c_void);
        } else {
            assert!((*ai).lidx.is_null() && !(*ai).is_slice);
        }

        if !(*ai).uidx.is_null() {
            subexpr = transformExpr(pstate, (*ai).uidx, (*pstate).p_expr_kind);
            /* If it's not int4 already, try to coerce */
            subexpr = coerce_to_target_type(
                pstate,
                subexpr,
                exprType(subexpr),
                INT4OID,
                -1,
                COERCION_ASSIGNMENT,
                COERCE_IMPLICIT_CAST,
                -1,
            );
            if subexpr.is_null() {
                ereport!(
                    ERROR,
                    "array subscript must have type integer"
                );
            }
        } else {
            /* Slice with omitted upper bound, put NULL into the list */
            assert!(isSlice && (*ai).is_slice);
            subexpr = std::ptr::null_mut();
        }
        upperIndexpr = lappend(upperIndexpr, subexpr as *mut std::ffi::c_void);
    });

    /* ... and store the transformed lists into the SubscriptRef node */
    (*sbsref).refupperindexpr = upperIndexpr;
    (*sbsref).reflowerindexpr = lowerIndexpr;

    /* Verify subscript list lengths are within implementation limit */
    if list_length(upperIndexpr) > MAXDIM {
        elog!(
            ERROR,
            "number of array dimensions ({}) exceeds the maximum allowed ({})",
            list_length(upperIndexpr),
            MAXDIM
        );
    }
    /* We need not check lowerIndexpr separately */

    /*
     * Determine the result type of the subscripting operation.  It's the same
     * as the array type if we're slicing, else it's the element type.  In
     * either case, the typmod is the same as the array's, so we need not
     * change reftypmod.
     */
    if isSlice {
        (*sbsref).refrestype = (*sbsref).refcontainertype;
    } else {
        (*sbsref).refrestype = (*sbsref).refelemtype;
    }
}

/*
 * During execution, process the subscripts in a SubscriptingRef expression.
 *
 * The subscript expressions are already evaluated in Datum form in the
 * SubscriptingRefState's arrays.  Check and convert them as necessary.
 *
 * If any subscript is NULL, we throw error in assignment cases, or in fetch
 * cases set result to NULL and return false (instructing caller to skip the
 * rest of the SubscriptingRef sequence).
 *
 * We convert all the subscripts to plain integers and save them in the
 * sbsrefstate->workspace arrays.
 */
unsafe fn array_subscript_check_subscripts(
    state: *mut ExprState,
    op: *mut ExprEvalStep,
    econtext: *mut ExprContext,
) -> bool {
    let sbsrefstate = (*op).d.sbsref_subscript.state;
    let workspace = (*sbsrefstate).workspace as *mut ArraySubWorkspace;

    /* Process upper subscripts */
    for i in 0..(*sbsrefstate).numupper as usize {
        if *(*sbsrefstate).upperprovided.add(i) {
            /* If any index expr yields NULL, result is NULL or error */
            if *(*sbsrefstate).upperindexnull.add(i) {
                if (*sbsrefstate).isassignment {
                    ereport!(
                        ERROR,
                        "array subscript in assignment must not be null"
                    );
                }
                *(*op).resnull = true;
                return false;
            }
            (*workspace).upperindex[i] = DatumGetInt32(*(*sbsrefstate).upperindex.add(i));
        }
    }

    /* Likewise for lower subscripts */
    for i in 0..(*sbsrefstate).numlower as usize {
        if *(*sbsrefstate).lowerprovided.add(i) {
            /* If any index expr yields NULL, result is NULL or error */
            if *(*sbsrefstate).lowerindexnull.add(i) {
                if (*sbsrefstate).isassignment {
                    ereport!(
                        ERROR,
                        "array subscript in assignment must not be null"
                    );
                }
                *(*op).resnull = true;
                return false;
            }
            (*workspace).lowerindex[i] = DatumGetInt32(*(*sbsrefstate).lowerindex.add(i));
        }
    }

    true
}

/*
 * Evaluate SubscriptingRef fetch for an array element.
 *
 * Source container is in step's result variable (it's known not NULL, since
 * we set fetch_strict to true), and indexes have already been evaluated into
 * workspace array.
 */
unsafe fn array_subscript_fetch(
    state: *mut ExprState,
    op: *mut ExprEvalStep,
    econtext: *mut ExprContext,
) {
    let sbsrefstate = (*op).d.sbsref.state;
    let workspace = (*sbsrefstate).workspace as *mut ArraySubWorkspace;

    /* Should not get here if source array (or any subscript) is null */
    assert!(!(*(*op).resnull));

    *(*op).resvalue = array_get_element(
        *(*op).resvalue,
        (*sbsrefstate).numupper,
        (*workspace).upperindex.as_mut_ptr(),
        (*workspace).refattrlength,
        (*workspace).refelemlength,
        (*workspace).refelembyval,
        (*workspace).refelemalign,
        (*op).resnull,
    );
}

/*
 * Evaluate SubscriptingRef fetch for an array slice.
 *
 * Source container is in step's result variable (it's known not NULL, since
 * we set fetch_strict to true), and indexes have already been evaluated into
 * workspace array.
 */
unsafe fn array_subscript_fetch_slice(
    state: *mut ExprState,
    op: *mut ExprEvalStep,
    econtext: *mut ExprContext,
) {
    let sbsrefstate = (*op).d.sbsref.state;
    let workspace = (*sbsrefstate).workspace as *mut ArraySubWorkspace;

    /* Should not get here if source array (or any subscript) is null */
    assert!(!(*(*op).resnull));

    *(*op).resvalue = array_get_slice(
        *(*op).resvalue,
        (*sbsrefstate).numupper,
        (*workspace).upperindex.as_mut_ptr(),
        (*workspace).lowerindex.as_mut_ptr(),
        (*sbsrefstate).upperprovided,
        (*sbsrefstate).lowerprovided,
        (*workspace).refattrlength,
        (*workspace).refelemlength,
        (*workspace).refelembyval,
        (*workspace).refelemalign,
    );
    /* The slice is never NULL, so no need to change *op->resnull */
}

/*
 * Evaluate SubscriptingRef assignment for an array element assignment.
 *
 * Input container (possibly null) is in result area, replacement value is in
 * SubscriptingRefState's replacevalue/replacenull.
 */
unsafe fn array_subscript_assign(
    state: *mut ExprState,
    op: *mut ExprEvalStep,
    econtext: *mut ExprContext,
) {
    let sbsrefstate = (*op).d.sbsref.state;
    let workspace = (*sbsrefstate).workspace as *mut ArraySubWorkspace;
    let mut arraySource: Datum = *(*op).resvalue;

    /*
     * For an assignment to a fixed-length array type, both the original array
     * and the value to be assigned into it must be non-NULL, else we punt and
     * return the original array.
     */
    if (*workspace).refattrlength > 0 {
        if *(*op).resnull || (*sbsrefstate).replacenull {
            return;
        }
    }

    /*
     * For assignment to varlena arrays, we handle a NULL original array by
     * substituting an empty (zero-dimensional) array; insertion of the new
     * element will result in a singleton array value.  It does not matter
     * whether the new element is NULL.
     */
    if *(*op).resnull {
        arraySource = PointerGetDatum(construct_empty_array((*workspace).refelemtype));
        *(*op).resnull = false;
    }

    *(*op).resvalue = array_set_element(
        arraySource,
        (*sbsrefstate).numupper,
        (*workspace).upperindex.as_mut_ptr(),
        (*sbsrefstate).replacevalue,
        (*sbsrefstate).replacenull,
        (*workspace).refattrlength,
        (*workspace).refelemlength,
        (*workspace).refelembyval,
        (*workspace).refelemalign,
    );
    /* The result is never NULL, so no need to change *op->resnull */
}

/*
 * Evaluate SubscriptingRef assignment for an array slice assignment.
 *
 * Input container (possibly null) is in result area, replacement value is in
 * SubscriptingRefState's replacevalue/replacenull.
 */
unsafe fn array_subscript_assign_slice(
    state: *mut ExprState,
    op: *mut ExprEvalStep,
    econtext: *mut ExprContext,
) {
    let sbsrefstate = (*op).d.sbsref.state;
    let workspace = (*sbsrefstate).workspace as *mut ArraySubWorkspace;
    let mut arraySource: Datum = *(*op).resvalue;

    /*
     * For an assignment to a fixed-length array type, both the original array
     * and the value to be assigned into it must be non-NULL, else we punt and
     * return the original array.
     */
    if (*workspace).refattrlength > 0 {
        if *(*op).resnull || (*sbsrefstate).replacenull {
            return;
        }
    }

    /*
     * For assignment to varlena arrays, we handle a NULL original array by
     * substituting an empty (zero-dimensional) array; insertion of the new
     * element will result in a singleton array value.  It does not matter
     * whether the new element is NULL.
     */
    if *(*op).resnull {
        arraySource = PointerGetDatum(construct_empty_array((*workspace).refelemtype));
        *(*op).resnull = false;
    }

    *(*op).resvalue = array_set_slice(
        arraySource,
        (*sbsrefstate).numupper,
        (*workspace).upperindex.as_mut_ptr(),
        (*workspace).lowerindex.as_mut_ptr(),
        (*sbsrefstate).upperprovided,
        (*sbsrefstate).lowerprovided,
        (*sbsrefstate).replacevalue,
        (*sbsrefstate).replacenull,
        (*workspace).refattrlength,
        (*workspace).refelemlength,
        (*workspace).refelembyval,
        (*workspace).refelemalign,
    );
    /* The result is never NULL, so no need to change *op->resnull */
}

/*
 * Compute old array element value for a SubscriptingRef assignment
 * expression.  Will only be called if the new-value subexpression
 * contains SubscriptingRef or FieldStore.  This is the same as the
 * regular fetch case, except that we have to handle a null array,
 * and the value should be stored into the SubscriptingRefState's
 * prevvalue/prevnull fields.
 */
unsafe fn array_subscript_fetch_old(
    state: *mut ExprState,
    op: *mut ExprEvalStep,
    econtext: *mut ExprContext,
) {
    let sbsrefstate = (*op).d.sbsref.state;
    let workspace = (*sbsrefstate).workspace as *mut ArraySubWorkspace;

    if *(*op).resnull {
        /* whole array is null, so any element is too */
        (*sbsrefstate).prevvalue = 0 as Datum;
        (*sbsrefstate).prevnull = true;
    } else {
        (*sbsrefstate).prevvalue = array_get_element(
            *(*op).resvalue,
            (*sbsrefstate).numupper,
            (*workspace).upperindex.as_mut_ptr(),
            (*workspace).refattrlength,
            (*workspace).refelemlength,
            (*workspace).refelembyval,
            (*workspace).refelemalign,
            &mut (*sbsrefstate).prevnull,
        );
    }
}

/*
 * Compute old array slice value for a SubscriptingRef assignment
 * expression.  Will only be called if the new-value subexpression
 * contains SubscriptingRef or FieldStore.  This is the same as the
 * regular fetch case, except that we have to handle a null array,
 * and the value should be stored into the SubscriptingRefState's
 * prevvalue/prevnull fields.
 *
 * Note: this is presently dead code, because the new value for a
 * slice would have to be an array, so it couldn't directly contain a
 * FieldStore; nor could it contain a SubscriptingRef assignment, since
 * we consider adjacent subscripts to index one multidimensional array
 * not nested array types.  Future generalizations might make this
 * reachable, however.
 */
unsafe fn array_subscript_fetch_old_slice(
    state: *mut ExprState,
    op: *mut ExprEvalStep,
    econtext: *mut ExprContext,
) {
    let sbsrefstate = (*op).d.sbsref.state;
    let workspace = (*sbsrefstate).workspace as *mut ArraySubWorkspace;

    if *(*op).resnull {
        /* whole array is null, so any slice is too */
        (*sbsrefstate).prevvalue = 0 as Datum;
        (*sbsrefstate).prevnull = true;
    } else {
        (*sbsrefstate).prevvalue = array_get_slice(
            *(*op).resvalue,
            (*sbsrefstate).numupper,
            (*workspace).upperindex.as_mut_ptr(),
            (*workspace).lowerindex.as_mut_ptr(),
            (*sbsrefstate).upperprovided,
            (*sbsrefstate).lowerprovided,
            (*workspace).refattrlength,
            (*workspace).refelemlength,
            (*workspace).refelembyval,
            (*workspace).refelemalign,
        );
        /* slices of non-null arrays are never null */
        (*sbsrefstate).prevnull = false;
    }
}

/*
 * Set up execution state for an array subscript operation.
 */
unsafe fn array_exec_setup(
    sbsref: *const SubscriptingRef,
    sbsrefstate: *mut SubscriptingRefState,
    methods: *mut SubscriptExecSteps,
) {
    let is_slice: bool = (*sbsrefstate).numlower != 0;
    let workspace: *mut ArraySubWorkspace;

    /*
     * Enforce the implementation limit on number of array subscripts.  This
     * check isn't entirely redundant with checking at parse time; conceivably
     * the expression was stored by a backend with a different MAXDIM value.
     */
    if (*sbsrefstate).numupper > MAXDIM {
        elog!(
            ERROR,
            "number of array dimensions ({}) exceeds the maximum allowed ({})",
            (*sbsrefstate).numupper,
            MAXDIM
        );
    }

    /* Should be impossible if parser is sane, but check anyway: */
    if (*sbsrefstate).numlower != 0 && (*sbsrefstate).numupper != (*sbsrefstate).numlower {
        elog!(ERROR, "upper and lower index lists are not same length");
    }

    /*
     * Allocate type-specific workspace.
     */
    workspace = palloc(std::mem::size_of::<ArraySubWorkspace>()) as *mut ArraySubWorkspace;
    (*sbsrefstate).workspace = workspace as *mut std::ffi::c_void;

    /*
     * Collect datatype details we'll need at execution.
     */
    (*workspace).refelemtype = (*sbsref).refelemtype;
    (*workspace).refattrlength = get_typlen((*sbsref).refcontainertype);
    get_typlenbyvalalign(
        (*sbsref).refelemtype,
        &mut (*workspace).refelemlength,
        &mut (*workspace).refelembyval,
        &mut (*workspace).refelemalign,
    );

    /*
     * Pass back pointers to appropriate step execution functions.
     */
    (*methods).sbs_check_subscripts = Some(array_subscript_check_subscripts);
    if is_slice {
        (*methods).sbs_fetch = Some(array_subscript_fetch_slice);
        (*methods).sbs_assign = Some(array_subscript_assign_slice);
        (*methods).sbs_fetch_old = Some(array_subscript_fetch_old_slice);
    } else {
        (*methods).sbs_fetch = Some(array_subscript_fetch);
        (*methods).sbs_assign = Some(array_subscript_assign);
        (*methods).sbs_fetch_old = Some(array_subscript_fetch_old);
    }
}

/*
 * array_subscript_handler
 *		Subscripting handler for standard varlena arrays.
 *
 * This should be used only for "true" array types, which have array headers
 * as understood by the varlena array routines, and are referenced by the
 * element type's pg_type.typarray field.
 */
#[no_mangle]
pub unsafe extern "C" fn array_subscript_handler(fcinfo: FunctionCallInfo) -> Datum {
    static SBSROUTINES: SubscriptRoutines = SubscriptRoutines {
        transform: Some(array_subscript_transform),
        exec_setup: Some(array_exec_setup),
        fetch_strict: true,    /* fetch returns NULL for NULL inputs */
        fetch_leakproof: true, /* fetch returns NULL for bad subscript */
        store_leakproof: false, /* ... but assignment throws error */
    };

    PG_RETURN_POINTER!(&SBSROUTINES as *const SubscriptRoutines as *mut std::ffi::c_void)
}

/*
 * raw_array_subscript_handler
 *		Subscripting handler for "raw" arrays.
 *
 * A "raw" array just contains N independent instances of the element type.
 * Currently we require both the element type and the array type to be fixed
 * length, but it wouldn't be too hard to relax that for the array type.
 *
 * As of now, all the support code is shared with standard varlena arrays.
 * We may split those into separate code paths, but probably that would yield
 * only marginal speedups.  The main point of having a separate handler is
 * so that pg_type.typsubscript clearly indicates the type's semantics.
 */
#[no_mangle]
pub unsafe extern "C" fn raw_array_subscript_handler(fcinfo: FunctionCallInfo) -> Datum {
    static SBSROUTINES: SubscriptRoutines = SubscriptRoutines {
        transform: Some(array_subscript_transform),
        exec_setup: Some(array_exec_setup),
        fetch_strict: true,    /* fetch returns NULL for NULL inputs */
        fetch_leakproof: true, /* fetch returns NULL for bad subscript */
        store_leakproof: false, /* ... but assignment throws error */
    };

    PG_RETURN_POINTER!(&SBSROUTINES as *const SubscriptRoutines as *mut std::ffi::c_void)
}

/*
 * array_subscript_handler_support()
 *
 * Planner support function for array_subscript_handler()
 */
#[no_mangle]
pub unsafe extern "C" fn array_subscript_handler_support(fcinfo: FunctionCallInfo) -> Datum {
    let rawreq = PG_GETARG_POINTER!(fcinfo, 0) as *mut Node;
    let mut ret: *mut Node = std::ptr::null_mut();

    if IsA!(rawreq, T_SupportRequestModifyInPlace) {
        /*
         * We can optimize in-place subscripted assignment if the refexpr is
         * the array being assigned to.  We don't need to worry about array
         * references within the refassgnexpr or the subscripts; however, if
         * there's no refassgnexpr then it's a fetch which there's no need to
         * optimize.
         */
        let req = rawreq as *mut SupportRequestModifyInPlace;
        let refexpr = linitial((*req).args) as *mut Param;

        if !refexpr.is_null()
            && IsA!(refexpr, T_Param)
            && (*refexpr).paramkind == PARAM_EXTERN
            && (*refexpr).paramid == (*req).paramid
            && !lsecond((*req).args).is_null()
        {
            ret = refexpr as *mut Node;
        }
    }

    PG_RETURN_POINTER!(ret as *mut std::ffi::c_void)
}

// ----------------------------------------------------------------------------
// Local stubs for as-yet-unported dependencies.
// ----------------------------------------------------------------------------

unsafe fn transformExpr(
    pstate: *mut ParseState,
    expr: *mut Node,
    exprKind: c_int,
) -> *mut Node {
    unimplemented!() // TODO: src/backend/parser/parse_expr.c
}

unsafe fn coerce_to_target_type(
    pstate: *mut ParseState,
    expr: *mut Node,
    exprtype: Oid,
    targettype: Oid,
    targettypmod: int32,
    ccontext: CoercionContext,
    cformat: CoercionForm,
    location: c_int,
) -> *mut Node {
    unimplemented!() // TODO: src/backend/parser/parse_coerce.c
}

unsafe fn exprType(expr: *const Node) -> Oid {
    unimplemented!() // TODO: src/backend/nodes/nodeFuncs.c
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
    unimplemented!() // TODO: src/backend/nodes/makefuncs.c
}

unsafe fn array_get_element(
    arraydatum: Datum,
    nSubscripts: c_int,
    indx: *mut c_int,
    arraytyplen: c_int,
    elmlen: c_int,
    elmbyval: bool,
    elmalign: c_char,
    isNull: *mut bool,
) -> Datum {
    unimplemented!() // TODO: src/backend/utils/adt/arrayfuncs.c
}

unsafe fn array_get_slice(
    arraydatum: Datum,
    nSubscripts: c_int,
    upperIndx: *mut c_int,
    lowerIndx: *mut c_int,
    upperProvided: *mut bool,
    lowerProvided: *mut bool,
    arraytyplen: c_int,
    elmlen: c_int,
    elmbyval: bool,
    elmalign: c_char,
) -> Datum {
    unimplemented!() // TODO: src/backend/utils/adt/arrayfuncs.c
}

unsafe fn array_set_element(
    arraydatum: Datum,
    nSubscripts: c_int,
    indx: *mut c_int,
    dataValue: Datum,
    isNull: bool,
    arraytyplen: c_int,
    elmlen: c_int,
    elmbyval: bool,
    elmalign: c_char,
) -> Datum {
    unimplemented!() // TODO: src/backend/utils/adt/arrayfuncs.c
}

unsafe fn array_set_slice(
    arraydatum: Datum,
    nSubscripts: c_int,
    upperIndx: *mut c_int,
    lowerIndx: *mut c_int,
    upperProvided: *mut bool,
    lowerProvided: *mut bool,
    srcArrayDatum: Datum,
    isNull: bool,
    arraytyplen: c_int,
    elmlen: c_int,
    elmbyval: bool,
    elmalign: c_char,
) -> Datum {
    unimplemented!() // TODO: src/backend/utils/adt/arrayfuncs.c
}

unsafe fn construct_empty_array(elmtype: Oid) -> *mut ArrayType {
    unimplemented!() // TODO: src/backend/utils/adt/arrayfuncs.c
}

unsafe fn get_typlen(typid: Oid) -> int16 {
    unimplemented!() // TODO: src/backend/utils/cache/lsyscache.c
}

unsafe fn get_typlenbyvalalign(
    typid: Oid,
    typlen: *mut int16,
    typbyval: *mut bool,
    typalign: *mut c_char,
) {
    unimplemented!() // TODO: src/backend/utils/cache/lsyscache.c
}
