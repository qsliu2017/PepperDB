//! jsonbsubs.rs
//!   Subscripting support functions for jsonb.
//!
//! Translated 1:1 from postgres/src/backend/utils/adt/jsonbsubs.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/utils/adt/jsonbsubs.c

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void};

use crate::c::int32;
use crate::postgres_ext::{InvalidOid, Oid};
use crate::postgres::DatumGetCString;

use crate::nodes::nodes::Node;
use crate::nodes::nodes::NodeTag::T_A_Indices;
use crate::nodes::pg_list::{lappend, lfirst, List, NIL};
use crate::nodes::parsenodes::A_Indices;
use crate::nodes::primnodes::SubscriptingRef;
use crate::nodes::primnodes::CoercionContext::{self, COERCION_IMPLICIT};
use crate::nodes::primnodes::CoercionForm::{self, COERCE_IMPLICIT_CAST};
use crate::nodes::execnodes::{ExprContext, ExprEvalStep, ExprState};
use crate::nodes::nodeFuncs::exprType;
use crate::parser::parse_node::{ParseExprKind, ParseState};
use crate::catalog::pg_type_d::{INT4OID, JSONBOID, TEXTOID, UNKNOWNOID};
use crate::utils::builtins::{format_type_be, CStringGetTextDatum};
use crate::utils::adt::int::int4out;
use crate::utils::palloc::palloc0;
use crate::utils::elog::ERROR;

// Pull in the local SubscriptingRefState / SubscriptExecSteps definitions and
// the SubscriptRoutines type the same way the array analogue (arraysubs.rs)
// does: SubscriptRoutines from nodes/subscripting, the executor-side state
// structs from arraysubs.rs (where they are faithfully defined until
// execExpr.h / execnodes.h are ported).
use crate::nodes::subscripting::SubscriptRoutines;
use crate::utils::adt::arraysubs::{SubscriptExecSteps, SubscriptingRefState};

use crate::{foreach, current_cell, lfirst_node, IsA, ereport, elog, Assert};
use crate::{DirectFunctionCall1, PG_RETURN_POINTER};

/* SubscriptingRefState.workspace for jsonb subscripting execution */
#[repr(C)]
pub struct JsonbSubWorkspace {
    pub expectArray: bool,     /* jsonb root is expected to be an array */
    pub indexOid: *mut Oid,    /* OID of coerced subscript expression, could
                                * be only integer or text */
    pub index: *mut Datum,     /* Subscript values in Datum format */
}

/*
 * Finish parse analysis of a SubscriptingRef expression for a jsonb.
 *
 * Transform the subscript expressions, coerce them to text,
 * and determine the result type of the SubscriptingRef node.
 */
unsafe fn jsonb_subscript_transform(
    sbsref: *mut SubscriptingRef,
    indirection: *mut List,
    pstate: *mut ParseState,
    isSlice: bool,
    isAssignment: bool,
) {
    let mut upperIndexpr: *mut List = NIL;

    /*
     * Transform and convert the subscript expressions. Jsonb subscripting
     * does not support slices, look only and the upper index.
     */
    foreach!(idx, indirection, {
        let ai = lfirst_node!(A_Indices, T_A_Indices, current_cell!(idx));
        let mut subExpr: *mut Node;

        if isSlice {
            let expr = if !(*ai).uidx.is_null() { (*ai).uidx } else { (*ai).lidx };

            ereport!(
                ERROR,
                "jsonb subscript does not support slices"
            );
            let _ = expr;
        }

        if !(*ai).uidx.is_null() {
            let subExprType: Oid;
            let mut targetType: Oid = UNKNOWNOID;

            subExpr = transformExpr(pstate, (*ai).uidx, (*pstate).p_expr_kind);
            subExprType = exprType(subExpr);

            if subExprType != UNKNOWNOID {
                let targets: [Oid; 2] = [INT4OID, TEXTOID];

                /*
                 * Jsonb can handle multiple subscript types, but cases when a
                 * subscript could be coerced to multiple target types must be
                 * avoided, similar to overloaded functions. It could be
                 * possibly extend with jsonpath in the future.
                 */
                for i in 0..2 {
                    if can_coerce_type(1, &subExprType, &targets[i], COERCION_IMPLICIT) {
                        /*
                         * One type has already succeeded, it means there are
                         * two coercion targets possible, failure.
                         */
                        if targetType != UNKNOWNOID {
                            ereport!(
                                ERROR,
                                format!(
                                    "subscript type {} is not supported",
                                    std::ffi::CStr::from_ptr(format_type_be(subExprType))
                                        .to_string_lossy()
                                )
                            );
                        }

                        targetType = targets[i];
                    }
                }

                /*
                 * No suitable types were found, failure.
                 */
                if targetType == UNKNOWNOID {
                    ereport!(
                        ERROR,
                        format!(
                            "subscript type {} is not supported",
                            std::ffi::CStr::from_ptr(format_type_be(subExprType))
                                .to_string_lossy()
                        )
                    );
                }
            } else {
                targetType = TEXTOID;
            }

            /*
             * We known from can_coerce_type that coercion will succeed, so
             * coerce_type could be used. Note the implicit coercion context,
             * which is required to handle subscripts of different types,
             * similar to overloaded functions.
             */
            subExpr = coerce_type(
                pstate,
                subExpr,
                subExprType,
                targetType,
                -1,
                COERCION_IMPLICIT,
                COERCE_IMPLICIT_CAST,
                -1,
            );
            if subExpr.is_null() {
                ereport!(
                    ERROR,
                    "jsonb subscript must have text type"
                );
            }
        } else {
            /*
             * Slice with omitted upper bound. Should not happen as we already
             * errored out on slice earlier, but handle this just in case.
             */
            Assert!(isSlice && (*ai).is_slice);
            ereport!(
                ERROR,
                "jsonb subscript does not support slices"
            );
        }

        upperIndexpr = lappend(upperIndexpr, subExpr as *mut c_void);
    });

    /* store the transformed lists into the SubscriptRef node */
    (*sbsref).refupperindexpr = upperIndexpr;
    (*sbsref).reflowerindexpr = NIL;

    /* Determine the result type of the subscripting operation; always jsonb */
    (*sbsref).refrestype = JSONBOID;
    (*sbsref).reftypmod = -1;
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
 */
unsafe fn jsonb_subscript_check_subscripts(
    state: *mut ExprState,
    op: *mut ExprEvalStep,
    econtext: *mut ExprContext,
) -> bool {
    let sbsrefstate = (*op).d.sbsref_subscript.state;
    let workspace = (*sbsrefstate).workspace as *mut JsonbSubWorkspace;

    /*
     * In case if the first subscript is an integer, the source jsonb is
     * expected to be an array. This information is not used directly, all
     * such cases are handled within corresponding jsonb assign functions. But
     * if the source jsonb is NULL the expected type will be used to construct
     * an empty source.
     */
    if (*sbsrefstate).numupper > 0
        && *(*sbsrefstate).upperprovided.add(0)
        && !*(*sbsrefstate).upperindexnull.add(0)
        && *(*workspace).indexOid.add(0) == INT4OID
    {
        (*workspace).expectArray = true;
    }

    /* Process upper subscripts */
    for i in 0..(*sbsrefstate).numupper as usize {
        if *(*sbsrefstate).upperprovided.add(i) {
            /* If any index expr yields NULL, result is NULL or error */
            if *(*sbsrefstate).upperindexnull.add(i) {
                if (*sbsrefstate).isassignment {
                    ereport!(
                        ERROR,
                        "jsonb subscript in assignment must not be null"
                    );
                }
                *(*op).resnull = true;
                return false;
            }

            /*
             * For jsonb fetch and assign functions we need to provide path in
             * text format. Convert if it's not already text.
             */
            if *(*workspace).indexOid.add(i) == INT4OID {
                let datum: Datum = *(*sbsrefstate).upperindex.add(i);
                let cs: *mut c_char = DatumGetCString(DirectFunctionCall1!(int4out, datum));

                *(*workspace).index.add(i) = CStringGetTextDatum(cs);
            } else {
                *(*workspace).index.add(i) = *(*sbsrefstate).upperindex.add(i);
            }
        }
    }

    true
}

/*
 * Evaluate SubscriptingRef fetch for a jsonb element.
 *
 * Source container is in step's result variable (it's known not NULL, since
 * we set fetch_strict to true).
 */
unsafe fn jsonb_subscript_fetch(
    state: *mut ExprState,
    op: *mut ExprEvalStep,
    econtext: *mut ExprContext,
) {
    let sbsrefstate = (*op).d.sbsref.state;
    let workspace = (*sbsrefstate).workspace as *mut JsonbSubWorkspace;
    let jsonbSource: *mut Jsonb;

    /* Should not get here if source jsonb (or any subscript) is null */
    Assert!(!(*(*op).resnull));

    jsonbSource = DatumGetJsonbP(*(*op).resvalue);
    *(*op).resvalue = jsonb_get_element(
        jsonbSource,
        (*workspace).index,
        (*sbsrefstate).numupper,
        (*op).resnull,
        false,
    );
}

/*
 * Evaluate SubscriptingRef assignment for a jsonb element assignment.
 *
 * Input container (possibly null) is in result area, replacement value is in
 * SubscriptingRefState's replacevalue/replacenull.
 */
unsafe fn jsonb_subscript_assign(
    state: *mut ExprState,
    op: *mut ExprEvalStep,
    econtext: *mut ExprContext,
) {
    let sbsrefstate = (*op).d.sbsref.state;
    let workspace = (*sbsrefstate).workspace as *mut JsonbSubWorkspace;
    let jsonbSource: *mut Jsonb;
    let mut replacevalue: JsonbValue = std::mem::zeroed();

    if (*sbsrefstate).replacenull {
        replacevalue.type_ = jbvNull;
    } else {
        JsonbToJsonbValue(DatumGetJsonbP((*sbsrefstate).replacevalue), &mut replacevalue);
    }

    /*
     * In case if the input container is null, set up an empty jsonb and
     * proceed with the assignment.
     */
    if *(*op).resnull {
        let mut newSource: JsonbValue = std::mem::zeroed();

        /*
         * To avoid any surprising results, set up an empty jsonb array in
         * case of an array is expected (i.e. the first subscript is integer),
         * otherwise jsonb object.
         */
        if (*workspace).expectArray {
            newSource.type_ = jbvArray;
            newSource.val.array.nElems = 0;
            newSource.val.array.rawScalar = false;
        } else {
            newSource.type_ = jbvObject;
            newSource.val.object.nPairs = 0;
        }

        jsonbSource = JsonbValueToJsonb(&mut newSource);
        *(*op).resnull = false;
    } else {
        jsonbSource = DatumGetJsonbP(*(*op).resvalue);
    }

    *(*op).resvalue = jsonb_set_element(
        jsonbSource,
        (*workspace).index,
        (*sbsrefstate).numupper,
        &mut replacevalue,
    );
    /* The result is never NULL, so no need to change *op->resnull */
}

/*
 * Compute old jsonb element value for a SubscriptingRef assignment
 * expression.  Will only be called if the new-value subexpression
 * contains SubscriptingRef or FieldStore.  This is the same as the
 * regular fetch case, except that we have to handle a null jsonb,
 * and the value should be stored into the SubscriptingRefState's
 * prevvalue/prevnull fields.
 */
unsafe fn jsonb_subscript_fetch_old(
    state: *mut ExprState,
    op: *mut ExprEvalStep,
    econtext: *mut ExprContext,
) {
    let sbsrefstate = (*op).d.sbsref.state;

    if *(*op).resnull {
        /* whole jsonb is null, so any element is too */
        (*sbsrefstate).prevvalue = 0 as Datum;
        (*sbsrefstate).prevnull = true;
    } else {
        let jsonbSource: *mut Jsonb = DatumGetJsonbP(*(*op).resvalue);

        (*sbsrefstate).prevvalue = jsonb_get_element(
            jsonbSource,
            (*sbsrefstate).upperindex,
            (*sbsrefstate).numupper,
            &mut (*sbsrefstate).prevnull,
            false,
        );
    }
}

/*
 * Set up execution state for a jsonb subscript operation. Opposite to the
 * arrays subscription, there is no limit for number of subscripts as jsonb
 * type itself doesn't have nesting limits.
 */
unsafe fn jsonb_exec_setup(
    sbsref: *const SubscriptingRef,
    sbsrefstate: *mut SubscriptingRefState,
    methods: *mut SubscriptExecSteps,
) {
    let workspace: *mut JsonbSubWorkspace;
    let nupper: c_int = (*(*sbsref).refupperindexpr).length;
    let mut ptr: *mut c_char;

    /* Allocate type-specific workspace with space for per-subscript data */
    workspace = palloc0(
        MAXALIGN(std::mem::size_of::<JsonbSubWorkspace>())
            + nupper as usize
                * (std::mem::size_of::<Datum>() + std::mem::size_of::<Oid>()),
    ) as *mut JsonbSubWorkspace;
    (*workspace).expectArray = false;
    ptr = (workspace as *mut c_char).add(MAXALIGN(std::mem::size_of::<JsonbSubWorkspace>()));

    /*
     * This coding assumes sizeof(Datum) >= sizeof(Oid), else we might
     * misalign the indexOid pointer
     */
    (*workspace).index = ptr as *mut Datum;
    ptr = ptr.add(nupper as usize * std::mem::size_of::<Datum>());
    (*workspace).indexOid = ptr as *mut Oid;

    (*sbsrefstate).workspace = workspace as *mut c_void;

    /* Collect subscript data types necessary at execution time */
    foreach!(lc, (*sbsref).refupperindexpr, {
        let expr = lfirst(current_cell!(lc)) as *mut Node;
        let i = foreach_current_index!(lc);

        *(*workspace).indexOid.add(i as usize) = exprType(expr);
    });

    /*
     * Pass back pointers to appropriate step execution functions.
     */
    (*methods).sbs_check_subscripts = Some(jsonb_subscript_check_subscripts);
    (*methods).sbs_fetch = Some(jsonb_subscript_fetch);
    (*methods).sbs_assign = Some(jsonb_subscript_assign);
    (*methods).sbs_fetch_old = Some(jsonb_subscript_fetch_old);
}

/*
 * jsonb_subscript_handler
 *		Subscripting handler for jsonb.
 *
 */
#[no_mangle]
pub unsafe extern "C" fn jsonb_subscript_handler(fcinfo: FunctionCallInfo) -> Datum {
    static SBSROUTINES: SubscriptRoutines = SubscriptRoutines {
        transform: Some(jsonb_subscript_transform),
        exec_setup: Some(jsonb_exec_setup),
        fetch_strict: true,     /* fetch returns NULL for NULL inputs */
        fetch_leakproof: true,  /* fetch returns NULL for bad subscript */
        store_leakproof: false, /* ... but assignment throws error */
    };

    PG_RETURN_POINTER!(&SBSROUTINES as *const SubscriptRoutines as *mut c_void)
}

// ----------------------------------------------------------------------------
// Local definitions / stubs for as-yet-unported dependencies.
// ----------------------------------------------------------------------------

/// MAXALIGN - round size up to MAXIMUM_ALIGNOF boundary.
///
// TODO(pg-port): real MAXALIGN lives in src/include/c.h.
#[inline]
fn MAXALIGN(len: usize) -> usize {
    const MAXIMUM_ALIGNOF: usize = 8;
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

// TODO(pg-port): real Jsonb / JsonbValue and the jbv* enum members live in
// src/include/utils/jsonb.h (utils/adt/jsonb.rs once ported).
#[repr(C)]
pub struct Jsonb {
    _opaque: [u8; 0],
}

const jbvNull: c_int = 0x0;
const jbvArray: c_int = 0x10;
const jbvObject: c_int = 0x20;

#[repr(C)]
pub struct JsonbValueArray {
    pub nElems: c_int,
    pub elems: *mut JsonbValue,
    pub rawScalar: bool,
}

#[repr(C)]
pub struct JsonbValueObject {
    pub nPairs: c_int,
    pub pairs: *mut c_void,
}

#[repr(C)]
pub union JsonbValueVal {
    pub array: std::mem::ManuallyDrop<JsonbValueArray>,
    pub object: std::mem::ManuallyDrop<JsonbValueObject>,
    _bytes: [u8; 24],
}

#[repr(C)]
pub struct JsonbValue {
    pub type_: c_int, /* C: enum jbvType type; */
    pub val: JsonbValueVal,
}

// TODO(pg-port): real DatumGetJsonbP lives in src/include/utils/jsonb.h.
unsafe fn DatumGetJsonbP(d: Datum) -> *mut Jsonb {
    let _ = d;
    unimplemented!() // TODO: src/include/utils/jsonb.h
}

// TODO(pg-port): real jsonb_get_element lives in src/backend/utils/adt/jsonbfuncs.c.
unsafe fn jsonb_get_element(
    jb: *mut Jsonb,
    path: *mut Datum,
    npath: c_int,
    isnull: *mut bool,
    as_text: bool,
) -> Datum {
    let _ = (jb, path, npath, isnull, as_text);
    unimplemented!() // TODO: src/backend/utils/adt/jsonbfuncs.c
}

// TODO(pg-port): real jsonb_set_element lives in src/backend/utils/adt/jsonfuncs.c.
unsafe fn jsonb_set_element(
    jb: *mut Jsonb,
    path: *mut Datum,
    path_len: c_int,
    newval: *mut JsonbValue,
) -> Datum {
    let _ = (jb, path, path_len, newval);
    unimplemented!() // TODO: src/backend/utils/adt/jsonfuncs.c
}

// TODO(pg-port): real JsonbToJsonbValue lives in src/backend/utils/adt/jsonb_util.c.
unsafe fn JsonbToJsonbValue(jsonb: *mut Jsonb, val: *mut JsonbValue) {
    let _ = (jsonb, val);
    unimplemented!() // TODO: src/backend/utils/adt/jsonb_util.c
}

// TODO(pg-port): real JsonbValueToJsonb lives in src/backend/utils/adt/jsonb_util.c.
unsafe fn JsonbValueToJsonb(val: *mut JsonbValue) -> *mut Jsonb {
    let _ = val;
    unimplemented!() // TODO: src/backend/utils/adt/jsonb_util.c
}

// TODO(pg-port): real transformExpr lives in src/backend/parser/parse_expr.c.
unsafe fn transformExpr(
    pstate: *mut ParseState,
    expr: *mut Node,
    exprKind: ParseExprKind,
) -> *mut Node {
    let _ = (pstate, expr, exprKind);
    unimplemented!() // TODO: src/backend/parser/parse_expr.c
}

// TODO(pg-port): real can_coerce_type lives in src/backend/parser/parse_coerce.c.
unsafe fn can_coerce_type(
    nargs: c_int,
    input_typeids: *const Oid,
    target_typeids: *const Oid,
    ccontext: CoercionContext,
) -> bool {
    let _ = (nargs, input_typeids, target_typeids, ccontext);
    unimplemented!() // TODO: src/backend/parser/parse_coerce.c
}

// TODO(pg-port): real coerce_type lives in src/backend/parser/parse_coerce.c.
unsafe fn coerce_type(
    pstate: *mut ParseState,
    node: *mut Node,
    inputTypeId: Oid,
    targetTypeId: Oid,
    targetTypeMod: int32,
    ccontext: CoercionContext,
    cformat: CoercionForm,
    location: c_int,
) -> *mut Node {
    let _ = (
        pstate,
        node,
        inputTypeId,
        targetTypeId,
        targetTypeMod,
        ccontext,
        cformat,
        location,
    );
    unimplemented!() // TODO: src/backend/parser/parse_coerce.c
}
