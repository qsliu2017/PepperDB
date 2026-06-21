//! Translation of postgres/src/include/nodes/nodeFuncs.h
//!                + postgres/src/backend/nodes/nodeFuncs.c
//!
//! Various general-purpose manipulations of Node trees.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! ---------------------------------------------------------------------------
//! Translation notes (deviations from the C source):
//!
//! * Every function that dereferences a node pointer is `pub unsafe fn`,
//!   matching the raw-pointer node model used throughout the port.
//!
//! * C identifiers are preserved verbatim, including the `_impl` suffixes the
//!   header gives the walker/mutator entry points.  The header's
//!   `expression_tree_walker(n, w, c)` style macros - which exist solely to cast
//!   a strongly-typed callback to the canonical `tree_walker_callback` /
//!   `tree_mutator_callback` type - are reproduced here as `#[inline] pub unsafe
//!   fn` wrappers (Rust has no implicit fn-pointer cast, and our callbacks are
//!   already the canonical type, so the wrapper is a thin pass-through).
//!
//! * Tree-walk/mutate callbacks are C function pointers.  They are modeled as:
//!     - walker:  `Option<unsafe extern "C" fn(*mut Node, *mut c_void) -> bool>`
//!     - mutator: `Option<unsafe extern "C" fn(*mut Node, *mut c_void) -> *mut Node>`
//!   ...except we use plain `unsafe fn` (not `extern "C"`) to match how other
//!   ported units pass Rust fn items; `None` models a NULL callback (never used
//!   by the C, but the type is nullable for fidelity).  The `WALK(n)` /
//!   `MUTATE(...)` / `LIST_WALK(l)` / `PSWALK(n)` helper macros from the C are
//!   reproduced as local `macro_rules!` with identical early-return semantics.
//!
//! * The big `switch (nodeTag(node))` statements become `match nodeTag(node)`
//!   over `NodeTag`, covering every case the C source has, casting `node` to the
//!   concrete AST struct pointer for each arm.
//!
//! * `exprType`/`exprCollation`/etc. are almost entirely plain field reads off
//!   the AST structs and are translated fully.  The handful of genuine catalog
//!   lookups (get_opcode, get_promoted_array_type, getTypeInputInfo,
//!   getTypeOutputInfo, format_type_be) live in utils/cache/lsyscache.c and
//!   utils/adt/format_type.c, which are not yet ported; they are declared here
//!   as private `// TODO(pg-port)` stubs that `unimplemented!()`.
//!
//! * A few stable, hardwired collation OIDs (C_COLLATION_OID,
//!   DEFAULT_COLLATION_OID) come from the generated catalog/pg_collation_d.h,
//!   which is not yet ported; they are defined locally with `// TODO(pg-port)`.
//!   Type OIDs (BOOLOID, INT4OID, ...) are imported from catalog::pg_type_d.

use crate::prelude::*; // Datum, Oid, int*, bool, OidIsValid, DatumGetInt32, Min, elog!/ereport!/errmsg!/Assert!
use crate::nodes::nodes::{nodeTag, Node, NodeTag};
use crate::nodes::pg_list::*; // List, ListCell, lfirst, foreach!, list_length, list_nth, NIL, lappend, list_copy, ...
use crate::nodes::primnodes::*;
use crate::nodes::parsenodes::*;
use crate::nodes::plannodes::*; // Plan + plan-node structs for the planstate walker
use crate::nodes::pathnodes::*; // PlaceHolderVar/Info, AppendRelInfo, IndexClause, RestrictInfo
use crate::nodes::execnodes::*; // PlanState + *State structs, innerPlanState/outerPlanState
use crate::{castNode, makeNode, IsA};
use crate::{lfirst_node, linitial_node};
use crate::{current_cell, for_each_from, foreach};
use core::ffi::{c_int, c_void};
use core::ptr::null_mut;

// Type OIDs live in the (partially) generated catalog header.
use crate::catalog::pg_type_d::{BOOLOID, INT4OID, NAMEOID, RECORDOID, TEXTOID, XMLOID};

// ----------------------------------------------------------------
//   Hardwired catalog constants referenced by this file.
// ----------------------------------------------------------------

/// `C_COLLATION_OID` - the "C" collation.
// TODO(pg-port): catalog/pg_collation_d.h
const C_COLLATION_OID: Oid = 950;

/// `DEFAULT_COLLATION_OID` - the database default collation.
// TODO(pg-port): catalog/pg_collation_d.h
const DEFAULT_COLLATION_OID: Oid = 100;

// ----------------------------------------------------------------
//   Flag bits for query_tree_walker and query_tree_mutator (nodeFuncs.h)
// ----------------------------------------------------------------

pub const QTW_IGNORE_RT_SUBQUERIES: c_int = 0x01; // subqueries in rtable
pub const QTW_IGNORE_CTE_SUBQUERIES: c_int = 0x02; // subqueries in cteList
pub const QTW_IGNORE_RC_SUBQUERIES: c_int = 0x03; // both of above
pub const QTW_IGNORE_JOINALIASES: c_int = 0x04; // JOIN alias var lists
pub const QTW_IGNORE_RANGE_TABLE: c_int = 0x08; // skip rangetable entirely
pub const QTW_EXAMINE_RTES_BEFORE: c_int = 0x10; // examine RTE nodes before their contents
pub const QTW_EXAMINE_RTES_AFTER: c_int = 0x20; // examine RTE nodes after their contents
pub const QTW_DONT_COPY_QUERY: c_int = 0x40; // do not copy top Query
pub const QTW_EXAMINE_SORTGROUP: c_int = 0x80; // include SortGroupClause lists
pub const QTW_IGNORE_GROUPEXPRS: c_int = 0x100; // GROUP expressions list

// ----------------------------------------------------------------
//   Callback function pointer types (nodeFuncs.h)
// ----------------------------------------------------------------

/// callback function for check_functions_in_node
pub type check_function_callback = Option<unsafe fn(func_id: Oid, context: *mut c_void) -> bool>;

/// callback functions for tree walkers
pub type tree_walker_callback = Option<unsafe fn(node: *mut Node, context: *mut c_void) -> bool>;
pub type planstate_tree_walker_callback =
    Option<unsafe fn(planstate: *mut PlanState, context: *mut c_void) -> bool>;

/// callback functions for tree mutators
pub type tree_mutator_callback =
    Option<unsafe fn(node: *mut Node, context: *mut c_void) -> *mut Node>;

// ----------------------------------------------------------------
//   Stubs for not-yet-translated helpers from other compilation units.
// ----------------------------------------------------------------

/// `check_stack_depth()` (miscadmin.h): guard against runaway recursion.
/// Not yet ported; a no-op here is safe for translation purposes.
// TODO(pg-port): tcop/postgres.c / miscadmin.h
#[inline]
fn check_stack_depth() {}

/// `get_opcode(opno)` (utils/cache/lsyscache.c): map operator OID -> proc OID.
///
/// # Safety
/// Trivially safe; stubbed pending lsyscache.
#[inline]
unsafe fn get_opcode(opno: Oid) -> Oid {
    crate::utils::cache::lsyscache::get_opcode(opno)
}

/// `get_promoted_array_type(typeid)` (utils/cache/lsyscache.c).
///
/// # Safety
/// Trivially safe; stubbed pending lsyscache.
#[inline]
unsafe fn get_promoted_array_type(_typeid: Oid) -> Oid {
    crate::utils::cache::lsyscache::get_promoted_array_type(_typeid)
}

/// `getTypeInputInfo(type, &typinput, &typioparam)` (utils/cache/lsyscache.c).
///
/// # Safety
/// Output pointers must be valid; stubbed pending lsyscache.
#[inline]
unsafe fn getTypeInputInfo(_typ: Oid, _typinput: *mut Oid, _typioparam: *mut Oid) {
    crate::utils::cache::lsyscache::getTypeInputInfo(_typ, _typinput, _typioparam)
}

/// `getTypeOutputInfo(type, &typoutput, &typisvarlena)` (utils/cache/lsyscache.c).
///
/// # Safety
/// Output pointers must be valid; stubbed pending lsyscache.
#[inline]
unsafe fn getTypeOutputInfo(_typ: Oid, _typoutput: *mut Oid, _typisvarlena: *mut bool) {
    crate::utils::cache::lsyscache::getTypeOutputInfo(_typ, _typoutput, _typisvarlena)
}

/// `format_type_be(type_oid)` (utils/adt/format_type.c): error-message helper.
///
/// # Safety
/// Trivially safe; stubbed pending format_type.
#[inline]
unsafe fn format_type_be(_type_oid: Oid) -> *mut c_char {
    crate::utils::adt::format_type::format_type_be(_type_oid)
}

/// `copyObject(node)` (nodes/copyfuncs.c): deep-copy an arbitrary Node.
///
/// # Safety
/// `node` must be NULL or a valid node pointer.
#[inline]
unsafe fn copyObject<T>(node: *const T) -> *mut T {
    crate::nodes::copyfuncs::copyObjectImpl(node as *const c_void) as *mut T
}

/// `ERRCODE_UNDEFINED_OBJECT` SQLSTATE class; used purely for classification.
// TODO(pg-port): utils/errcodes.h
const ERRCODE_UNDEFINED_OBJECT: c_int = 0;

/*
 *	exprType -
 *	  returns the Oid of the type of the expression's result.
 */
pub unsafe fn exprType(expr: *const Node) -> Oid {
    let r#type: Oid;

    if expr.is_null() {
        return InvalidOid;
    }

    match nodeTag(expr) {
        NodeTag::T_Var => {
            r#type = (*(expr as *const Var)).vartype;
        }
        NodeTag::T_Const => {
            r#type = (*(expr as *const Const)).consttype;
        }
        NodeTag::T_Param => {
            r#type = (*(expr as *const Param)).paramtype;
        }
        NodeTag::T_Aggref => {
            r#type = (*(expr as *const Aggref)).aggtype;
        }
        NodeTag::T_GroupingFunc => {
            r#type = INT4OID;
        }
        NodeTag::T_WindowFunc => {
            r#type = (*(expr as *const WindowFunc)).wintype;
        }
        NodeTag::T_MergeSupportFunc => {
            r#type = (*(expr as *const MergeSupportFunc)).msftype;
        }
        NodeTag::T_SubscriptingRef => {
            r#type = (*(expr as *const SubscriptingRef)).refrestype;
        }
        NodeTag::T_FuncExpr => {
            r#type = (*(expr as *const FuncExpr)).funcresulttype;
        }
        NodeTag::T_NamedArgExpr => {
            r#type = exprType((*(expr as *const NamedArgExpr)).arg as *const Node);
        }
        NodeTag::T_OpExpr => {
            r#type = (*(expr as *const OpExpr)).opresulttype;
        }
        NodeTag::T_DistinctExpr => {
            r#type = (*(expr as *const DistinctExpr)).opresulttype;
        }
        NodeTag::T_NullIfExpr => {
            r#type = (*(expr as *const NullIfExpr)).opresulttype;
        }
        NodeTag::T_ScalarArrayOpExpr => {
            r#type = BOOLOID;
        }
        NodeTag::T_BoolExpr => {
            r#type = BOOLOID;
        }
        NodeTag::T_SubLink => {
            let sublink = expr as *const SubLink;

            if (*sublink).subLinkType == EXPR_SUBLINK
                || (*sublink).subLinkType == ARRAY_SUBLINK
            {
                /* get the type of the subselect's first target column */
                let qtree = (*sublink).subselect as *mut Query;
                let tent: *mut TargetEntry;

                if qtree.is_null() || !IsA!(qtree, T_Query) {
                    elog!(ERROR, "cannot get type for untransformed sublink");
                }
                tent = linitial_node!(TargetEntry, T_TargetEntry, (*qtree).targetList);
                Assert!(!(*tent).resjunk);
                r#type = exprType((*tent).expr as *const Node);
                if (*sublink).subLinkType == ARRAY_SUBLINK {
                    let t = get_promoted_array_type(r#type);
                    if !OidIsValid(t) {
                        ereport!(
                            ERROR,
                            errmsg!(
                                "could not find array type for data type {:?}",
                                format_type_be(exprType((*tent).expr as *const Node))
                            )
                        );
                    }
                    let _ = errcode(ERRCODE_UNDEFINED_OBJECT);
                    return t;
                }
            } else if (*sublink).subLinkType == MULTIEXPR_SUBLINK {
                /* MULTIEXPR is always considered to return RECORD */
                r#type = RECORDOID;
            } else {
                /* for all other sublink types, result is boolean */
                r#type = BOOLOID;
            }
        }
        NodeTag::T_SubPlan => {
            let subplan = expr as *const SubPlan;

            if (*subplan).subLinkType == EXPR_SUBLINK
                || (*subplan).subLinkType == ARRAY_SUBLINK
            {
                /* get the type of the subselect's first target column */
                let mut t = (*subplan).firstColType;
                if (*subplan).subLinkType == ARRAY_SUBLINK {
                    t = get_promoted_array_type(t);
                    if !OidIsValid(t) {
                        ereport!(
                            ERROR,
                            errmsg!(
                                "could not find array type for data type {:?}",
                                format_type_be((*subplan).firstColType)
                            )
                        );
                    }
                    let _ = errcode(ERRCODE_UNDEFINED_OBJECT);
                }
                r#type = t;
            } else if (*subplan).subLinkType == MULTIEXPR_SUBLINK {
                /* MULTIEXPR is always considered to return RECORD */
                r#type = RECORDOID;
            } else {
                /* for all other subplan types, result is boolean */
                r#type = BOOLOID;
            }
        }
        NodeTag::T_AlternativeSubPlan => {
            let asplan = expr as *const AlternativeSubPlan;

            /* subplans should all return the same thing */
            r#type = exprType(linitial((*asplan).subplans) as *const Node);
        }
        NodeTag::T_FieldSelect => {
            r#type = (*(expr as *const FieldSelect)).resulttype;
        }
        NodeTag::T_FieldStore => {
            r#type = (*(expr as *const FieldStore)).resulttype;
        }
        NodeTag::T_RelabelType => {
            r#type = (*(expr as *const RelabelType)).resulttype;
        }
        NodeTag::T_CoerceViaIO => {
            r#type = (*(expr as *const CoerceViaIO)).resulttype;
        }
        NodeTag::T_ArrayCoerceExpr => {
            r#type = (*(expr as *const ArrayCoerceExpr)).resulttype;
        }
        NodeTag::T_ConvertRowtypeExpr => {
            r#type = (*(expr as *const ConvertRowtypeExpr)).resulttype;
        }
        NodeTag::T_CollateExpr => {
            r#type = exprType((*(expr as *const CollateExpr)).arg as *const Node);
        }
        NodeTag::T_CaseExpr => {
            r#type = (*(expr as *const CaseExpr)).casetype;
        }
        NodeTag::T_CaseTestExpr => {
            r#type = (*(expr as *const CaseTestExpr)).typeId;
        }
        NodeTag::T_ArrayExpr => {
            r#type = (*(expr as *const ArrayExpr)).array_typeid;
        }
        NodeTag::T_RowExpr => {
            r#type = (*(expr as *const RowExpr)).row_typeid;
        }
        NodeTag::T_RowCompareExpr => {
            r#type = BOOLOID;
        }
        NodeTag::T_CoalesceExpr => {
            r#type = (*(expr as *const CoalesceExpr)).coalescetype;
        }
        NodeTag::T_MinMaxExpr => {
            r#type = (*(expr as *const MinMaxExpr)).minmaxtype;
        }
        NodeTag::T_SQLValueFunction => {
            r#type = (*(expr as *const SQLValueFunction)).r#type;
        }
        NodeTag::T_XmlExpr => {
            if (*(expr as *const XmlExpr)).op == IS_DOCUMENT {
                r#type = BOOLOID;
            } else if (*(expr as *const XmlExpr)).op == IS_XMLSERIALIZE {
                r#type = TEXTOID;
            } else {
                r#type = XMLOID;
            }
        }
        NodeTag::T_JsonValueExpr => {
            let jve = expr as *const JsonValueExpr;

            r#type = exprType((*jve).formatted_expr as *const Node);
        }
        NodeTag::T_JsonConstructorExpr => {
            r#type = (*(*(expr as *const JsonConstructorExpr)).returning).typid;
        }
        NodeTag::T_JsonIsPredicate => {
            r#type = BOOLOID;
        }
        NodeTag::T_JsonExpr => {
            let jexpr = expr as *const JsonExpr;

            r#type = (*(*jexpr).returning).typid;
        }
        NodeTag::T_JsonBehavior => {
            let behavior = expr as *const JsonBehavior;

            r#type = exprType((*behavior).expr);
        }
        NodeTag::T_NullTest => {
            r#type = BOOLOID;
        }
        NodeTag::T_BooleanTest => {
            r#type = BOOLOID;
        }
        NodeTag::T_CoerceToDomain => {
            r#type = (*(expr as *const CoerceToDomain)).resulttype;
        }
        NodeTag::T_CoerceToDomainValue => {
            r#type = (*(expr as *const CoerceToDomainValue)).typeId;
        }
        NodeTag::T_SetToDefault => {
            r#type = (*(expr as *const SetToDefault)).typeId;
        }
        NodeTag::T_CurrentOfExpr => {
            r#type = BOOLOID;
        }
        NodeTag::T_NextValueExpr => {
            r#type = (*(expr as *const NextValueExpr)).typeId;
        }
        NodeTag::T_InferenceElem => {
            let n = expr as *const InferenceElem;

            r#type = exprType((*n).expr as *const Node);
        }
        NodeTag::T_ReturningExpr => {
            r#type = exprType((*(expr as *const ReturningExpr)).retexpr as *const Node);
        }
        NodeTag::T_PlaceHolderVar => {
            r#type = exprType((*(expr as *const PlaceHolderVar)).phexpr as *const Node);
        }
        _ => {
            elog!(ERROR, "unrecognized node type: {}", nodeTag(expr) as c_int);
            #[allow(unreachable_code)]
            {
                return InvalidOid; /* keep compiler quiet */
            }
        }
    }
    r#type
}

/*
 *	exprTypmod -
 *	  returns the type-specific modifier of the expression's result type,
 *	  if it can be determined.  In many cases, it can't and we return -1.
 */
pub unsafe fn exprTypmod(expr: *const Node) -> int32 {
    if expr.is_null() {
        return -1;
    }

    match nodeTag(expr) {
        NodeTag::T_Var => return (*(expr as *const Var)).vartypmod,
        NodeTag::T_Const => return (*(expr as *const Const)).consttypmod,
        NodeTag::T_Param => return (*(expr as *const Param)).paramtypmod,
        NodeTag::T_SubscriptingRef => return (*(expr as *const SubscriptingRef)).reftypmod,
        NodeTag::T_FuncExpr => {
            let mut coercedTypmod: int32 = 0;

            /* Be smart about length-coercion functions... */
            if exprIsLengthCoercion(expr, &mut coercedTypmod) {
                return coercedTypmod;
            }
        }
        NodeTag::T_NamedArgExpr => {
            return exprTypmod((*(expr as *const NamedArgExpr)).arg as *const Node)
        }
        NodeTag::T_NullIfExpr => {
            /*
             * Result is either first argument or NULL, so we can report
             * first argument's typmod if known.
             */
            let nexpr = expr as *const NullIfExpr;

            return exprTypmod(linitial((*nexpr).args) as *const Node);
        }
        NodeTag::T_SubLink => {
            let sublink = expr as *const SubLink;

            if (*sublink).subLinkType == EXPR_SUBLINK
                || (*sublink).subLinkType == ARRAY_SUBLINK
            {
                /* get the typmod of the subselect's first target column */
                let qtree = (*sublink).subselect as *mut Query;
                let tent: *mut TargetEntry;

                if qtree.is_null() || !IsA!(qtree, T_Query) {
                    elog!(ERROR, "cannot get type for untransformed sublink");
                }
                tent = linitial_node!(TargetEntry, T_TargetEntry, (*qtree).targetList);
                Assert!(!(*tent).resjunk);
                return exprTypmod((*tent).expr as *const Node);
                /* note we don't need to care if it's an array */
            }
            /* otherwise, result is RECORD or BOOLEAN, typmod is -1 */
        }
        NodeTag::T_SubPlan => {
            let subplan = expr as *const SubPlan;

            if (*subplan).subLinkType == EXPR_SUBLINK
                || (*subplan).subLinkType == ARRAY_SUBLINK
            {
                /* get the typmod of the subselect's first target column */
                /* note we don't need to care if it's an array */
                return (*subplan).firstColTypmod;
            }
            /* otherwise, result is RECORD or BOOLEAN, typmod is -1 */
        }
        NodeTag::T_AlternativeSubPlan => {
            let asplan = expr as *const AlternativeSubPlan;

            /* subplans should all return the same thing */
            return exprTypmod(linitial((*asplan).subplans) as *const Node);
        }
        NodeTag::T_FieldSelect => return (*(expr as *const FieldSelect)).resulttypmod,
        NodeTag::T_RelabelType => return (*(expr as *const RelabelType)).resulttypmod,
        NodeTag::T_ArrayCoerceExpr => return (*(expr as *const ArrayCoerceExpr)).resulttypmod,
        NodeTag::T_CollateExpr => {
            return exprTypmod((*(expr as *const CollateExpr)).arg as *const Node)
        }
        NodeTag::T_CaseExpr => {
            /*
             * If all the alternatives agree on type/typmod, return that
             * typmod, else use -1
             */
            let cexpr = expr as *const CaseExpr;
            let casetype: Oid = (*cexpr).casetype;
            let typmod: int32;

            if (*cexpr).defresult.is_null() {
                return -1;
            }
            if exprType((*cexpr).defresult as *const Node) != casetype {
                return -1;
            }
            typmod = exprTypmod((*cexpr).defresult as *const Node);
            if typmod < 0 {
                return -1; /* no point in trying harder */
            }
            foreach!(arg, (*cexpr).args, {
                let w = lfirst_node!(CaseWhen, T_CaseWhen, current_cell!(arg));

                if exprType((*w).result as *const Node) != casetype {
                    return -1;
                }
                if exprTypmod((*w).result as *const Node) != typmod {
                    return -1;
                }
            });
            return typmod;
        }
        NodeTag::T_CaseTestExpr => return (*(expr as *const CaseTestExpr)).typeMod,
        NodeTag::T_ArrayExpr => {
            /*
             * If all the elements agree on type/typmod, return that
             * typmod, else use -1
             */
            let arrayexpr = expr as *const ArrayExpr;
            let commontype: Oid;
            let typmod: int32;

            if (*arrayexpr).elements == NIL {
                return -1;
            }
            typmod = exprTypmod(linitial((*arrayexpr).elements) as *const Node);
            if typmod < 0 {
                return -1; /* no point in trying harder */
            }
            if (*arrayexpr).multidims {
                commontype = (*arrayexpr).array_typeid;
            } else {
                commontype = (*arrayexpr).element_typeid;
            }
            foreach!(elem, (*arrayexpr).elements, {
                let e = lfirst(current_cell!(elem)) as *mut Node;

                if exprType(e) != commontype {
                    return -1;
                }
                if exprTypmod(e) != typmod {
                    return -1;
                }
            });
            return typmod;
        }
        NodeTag::T_CoalesceExpr => {
            /*
             * If all the alternatives agree on type/typmod, return that
             * typmod, else use -1
             */
            let cexpr = expr as *const CoalesceExpr;
            let coalescetype: Oid = (*cexpr).coalescetype;
            let typmod: int32;

            if exprType(linitial((*cexpr).args) as *const Node) != coalescetype {
                return -1;
            }
            typmod = exprTypmod(linitial((*cexpr).args) as *const Node);
            if typmod < 0 {
                return -1; /* no point in trying harder */
            }
            for_each_from!(arg, (*cexpr).args, 1, {
                let e = lfirst(current_cell!(arg)) as *mut Node;

                if exprType(e) != coalescetype {
                    return -1;
                }
                if exprTypmod(e) != typmod {
                    return -1;
                }
            });
            return typmod;
        }
        NodeTag::T_MinMaxExpr => {
            /*
             * If all the alternatives agree on type/typmod, return that
             * typmod, else use -1
             */
            let mexpr = expr as *const MinMaxExpr;
            let minmaxtype: Oid = (*mexpr).minmaxtype;
            let typmod: int32;

            if exprType(linitial((*mexpr).args) as *const Node) != minmaxtype {
                return -1;
            }
            typmod = exprTypmod(linitial((*mexpr).args) as *const Node);
            if typmod < 0 {
                return -1; /* no point in trying harder */
            }
            for_each_from!(arg, (*mexpr).args, 1, {
                let e = lfirst(current_cell!(arg)) as *mut Node;

                if exprType(e) != minmaxtype {
                    return -1;
                }
                if exprTypmod(e) != typmod {
                    return -1;
                }
            });
            return typmod;
        }
        NodeTag::T_SQLValueFunction => return (*(expr as *const SQLValueFunction)).typmod,
        NodeTag::T_JsonValueExpr => {
            return exprTypmod((*(expr as *const JsonValueExpr)).formatted_expr as *const Node)
        }
        NodeTag::T_JsonConstructorExpr => {
            return (*(*(expr as *const JsonConstructorExpr)).returning).typmod
        }
        NodeTag::T_JsonExpr => {
            let jexpr = expr as *const JsonExpr;

            return (*(*jexpr).returning).typmod;
        }
        NodeTag::T_JsonBehavior => {
            let behavior = expr as *const JsonBehavior;

            return exprTypmod((*behavior).expr);
        }
        NodeTag::T_CoerceToDomain => return (*(expr as *const CoerceToDomain)).resulttypmod,
        NodeTag::T_CoerceToDomainValue => return (*(expr as *const CoerceToDomainValue)).typeMod,
        NodeTag::T_SetToDefault => return (*(expr as *const SetToDefault)).typeMod,
        NodeTag::T_ReturningExpr => {
            return exprTypmod((*(expr as *const ReturningExpr)).retexpr as *const Node)
        }
        NodeTag::T_PlaceHolderVar => {
            return exprTypmod((*(expr as *const PlaceHolderVar)).phexpr as *const Node)
        }
        _ => {}
    }
    -1
}

/*
 * exprIsLengthCoercion
 *		Detect whether an expression tree is an application of a datatype's
 *		typmod-coercion function.  Optionally extract the result's typmod.
 *
 * If coercedTypmod is not NULL, the typmod is stored there if the expression
 * is a length-coercion function, else -1 is stored there.
 *
 * Note that a combined type-and-length coercion will be treated as a
 * length coercion by this routine.
 */
pub unsafe fn exprIsLengthCoercion(expr: *const Node, coercedTypmod: *mut int32) -> bool {
    if !coercedTypmod.is_null() {
        *coercedTypmod = -1; /* default result on failure */
    }

    /*
     * Scalar-type length coercions are FuncExprs, array-type length coercions
     * are ArrayCoerceExprs
     */
    if !expr.is_null() && IsA!(expr, T_FuncExpr) {
        let func = expr as *const FuncExpr;
        let nargs: c_int;
        let second_arg: *mut Const;

        /*
         * If it didn't come from a coercion context, reject.
         */
        if (*func).funcformat != COERCE_EXPLICIT_CAST
            && (*func).funcformat != COERCE_IMPLICIT_CAST
        {
            return false;
        }

        /*
         * If it's not a two-argument or three-argument function with the
         * second argument being an int4 constant, it can't have been created
         * from a length coercion (it must be a type coercion, instead).
         */
        nargs = list_length((*func).args);
        if nargs < 2 || nargs > 3 {
            return false;
        }

        second_arg = lsecond((*func).args) as *mut Const;
        if !IsA!(second_arg, T_Const)
            || (*second_arg).consttype != INT4OID
            || (*second_arg).constisnull
        {
            return false;
        }

        /*
         * OK, it is indeed a length-coercion function.
         */
        if !coercedTypmod.is_null() {
            *coercedTypmod = DatumGetInt32((*second_arg).constvalue);
        }

        return true;
    }

    if !expr.is_null() && IsA!(expr, T_ArrayCoerceExpr) {
        let acoerce = expr as *const ArrayCoerceExpr;

        /* It's not a length coercion unless there's a nondefault typmod */
        if (*acoerce).resulttypmod < 0 {
            return false;
        }

        /*
         * OK, it is indeed a length-coercion expression.
         */
        if !coercedTypmod.is_null() {
            *coercedTypmod = (*acoerce).resulttypmod;
        }

        return true;
    }

    false
}

/*
 * applyRelabelType
 *		Add a RelabelType node if needed to make the expression expose
 *		the specified type, typmod, and collation.
 *
 * This is primarily intended to be used during planning.  Therefore, it must
 * maintain the post-eval_const_expressions invariants that there are not
 * adjacent RelabelTypes, and that the tree is fully const-folded (hence,
 * we mustn't return a RelabelType atop a Const).  If we do find a Const,
 * we'll modify it in-place if "overwrite_ok" is true; that should only be
 * passed as true if caller knows the Const is newly generated.
 */
pub unsafe fn applyRelabelType(
    mut arg: *mut Node,
    rtype: Oid,
    rtypmod: int32,
    rcollid: Oid,
    rformat: CoercionForm,
    rlocation: c_int,
    overwrite_ok: bool,
) -> *mut Node {
    /*
     * If we find stacked RelabelTypes (eg, from foo::int::oid) we can discard
     * all but the top one, and must do so to ensure that semantically
     * equivalent expressions are equal().
     */
    while !arg.is_null() && IsA!(arg, T_RelabelType) {
        arg = (*(arg as *mut RelabelType)).arg as *mut Node;
    }

    if !arg.is_null() && IsA!(arg, T_Const) {
        /* Modify the Const directly to preserve const-flatness. */
        let mut con = arg as *mut Const;

        if !overwrite_ok {
            con = copyObject(con);
        }
        (*con).consttype = rtype;
        (*con).consttypmod = rtypmod;
        (*con).constcollid = rcollid;
        /* We keep the Const's original location. */
        con as *mut Node
    } else if exprType(arg) == rtype
        && exprTypmod(arg) == rtypmod
        && exprCollation(arg) == rcollid
    {
        /* Sometimes we find a nest of relabels that net out to nothing. */
        arg
    } else {
        /* Nope, gotta have a RelabelType. */
        let newrelabel: *mut RelabelType = makeNode!(RelabelType, T_RelabelType);

        (*newrelabel).arg = arg as *mut Expr;
        (*newrelabel).resulttype = rtype;
        (*newrelabel).resulttypmod = rtypmod;
        (*newrelabel).resultcollid = rcollid;
        (*newrelabel).relabelformat = rformat;
        (*newrelabel).location = rlocation;
        newrelabel as *mut Node
    }
}

/*
 * relabel_to_typmod
 *		Add a RelabelType node that changes just the typmod of the expression.
 *
 * Convenience function for a common usage of applyRelabelType.
 */
pub unsafe fn relabel_to_typmod(expr: *mut Node, typmod: int32) -> *mut Node {
    applyRelabelType(
        expr,
        exprType(expr),
        typmod,
        exprCollation(expr),
        COERCE_EXPLICIT_CAST,
        -1,
        false,
    )
}

/*
 * strip_implicit_coercions: remove implicit coercions at top level of tree
 *
 * This doesn't modify or copy the input expression tree, just return a
 * pointer to a suitable place within it.
 *
 * Note: there isn't any useful thing we can do with a RowExpr here, so
 * just return it unchanged, even if it's marked as an implicit coercion.
 */
pub unsafe fn strip_implicit_coercions(node: *mut Node) -> *mut Node {
    if node.is_null() {
        return null_mut();
    }
    if IsA!(node, T_FuncExpr) {
        let f = node as *mut FuncExpr;

        if (*f).funcformat == COERCE_IMPLICIT_CAST {
            return strip_implicit_coercions(linitial((*f).args) as *mut Node);
        }
    } else if IsA!(node, T_RelabelType) {
        let r = node as *mut RelabelType;

        if (*r).relabelformat == COERCE_IMPLICIT_CAST {
            return strip_implicit_coercions((*r).arg as *mut Node);
        }
    } else if IsA!(node, T_CoerceViaIO) {
        let c = node as *mut CoerceViaIO;

        if (*c).coerceformat == COERCE_IMPLICIT_CAST {
            return strip_implicit_coercions((*c).arg as *mut Node);
        }
    } else if IsA!(node, T_ArrayCoerceExpr) {
        let c = node as *mut ArrayCoerceExpr;

        if (*c).coerceformat == COERCE_IMPLICIT_CAST {
            return strip_implicit_coercions((*c).arg as *mut Node);
        }
    } else if IsA!(node, T_ConvertRowtypeExpr) {
        let c = node as *mut ConvertRowtypeExpr;

        if (*c).convertformat == COERCE_IMPLICIT_CAST {
            return strip_implicit_coercions((*c).arg as *mut Node);
        }
    } else if IsA!(node, T_CoerceToDomain) {
        let c = node as *mut CoerceToDomain;

        if (*c).coercionformat == COERCE_IMPLICIT_CAST {
            return strip_implicit_coercions((*c).arg as *mut Node);
        }
    }
    node
}

/*
 * expression_returns_set
 *	  Test whether an expression returns a set result.
 *
 * Because we use expression_tree_walker(), this can also be applied to
 * whole targetlists; it'll produce true if any one of the tlist items
 * returns a set.
 */
pub unsafe fn expression_returns_set(clause: *mut Node) -> bool {
    expression_returns_set_walker(clause, null_mut())
}

unsafe fn expression_returns_set_walker(node: *mut Node, context: *mut c_void) -> bool {
    if node.is_null() {
        return false;
    }
    if IsA!(node, T_FuncExpr) {
        let expr = node as *mut FuncExpr;

        if (*expr).funcretset {
            return true;
        }
        /* else fall through to check args */
    }
    if IsA!(node, T_OpExpr) {
        let expr = node as *mut OpExpr;

        if (*expr).opretset {
            return true;
        }
        /* else fall through to check args */
    }

    /*
     * If you add any more cases that return sets, also fix
     * expression_returns_set_rows() in clauses.c and IS_SRF_CALL() in
     * tlist.c.
     */

    /* Avoid recursion for some cases that parser checks not to return a set */
    if IsA!(node, T_Aggref) {
        return false;
    }
    if IsA!(node, T_GroupingFunc) {
        return false;
    }
    if IsA!(node, T_WindowFunc) {
        return false;
    }

    expression_tree_walker(node, Some(expression_returns_set_walker), context)
}

/*
 *	exprCollation -
 *	  returns the Oid of the collation of the expression's result.
 *
 * Note: expression nodes that can invoke functions generally have an
 * "inputcollid" field, which is what the function should use as collation.
 * That is the resolved common collation of the node's inputs.  It is often
 * but not always the same as the result collation; in particular, if the
 * function produces a non-collatable result type from collatable inputs
 * or vice versa, the two are different.
 */
pub unsafe fn exprCollation(expr: *const Node) -> Oid {
    let coll: Oid;

    if expr.is_null() {
        return InvalidOid;
    }

    match nodeTag(expr) {
        NodeTag::T_Var => {
            coll = (*(expr as *const Var)).varcollid;
        }
        NodeTag::T_Const => {
            coll = (*(expr as *const Const)).constcollid;
        }
        NodeTag::T_Param => {
            coll = (*(expr as *const Param)).paramcollid;
        }
        NodeTag::T_Aggref => {
            coll = (*(expr as *const Aggref)).aggcollid;
        }
        NodeTag::T_GroupingFunc => {
            coll = InvalidOid;
        }
        NodeTag::T_WindowFunc => {
            coll = (*(expr as *const WindowFunc)).wincollid;
        }
        NodeTag::T_MergeSupportFunc => {
            coll = (*(expr as *const MergeSupportFunc)).msfcollid;
        }
        NodeTag::T_SubscriptingRef => {
            coll = (*(expr as *const SubscriptingRef)).refcollid;
        }
        NodeTag::T_FuncExpr => {
            coll = (*(expr as *const FuncExpr)).funccollid;
        }
        NodeTag::T_NamedArgExpr => {
            coll = exprCollation((*(expr as *const NamedArgExpr)).arg as *const Node);
        }
        NodeTag::T_OpExpr => {
            coll = (*(expr as *const OpExpr)).opcollid;
        }
        NodeTag::T_DistinctExpr => {
            coll = (*(expr as *const DistinctExpr)).opcollid;
        }
        NodeTag::T_NullIfExpr => {
            coll = (*(expr as *const NullIfExpr)).opcollid;
        }
        NodeTag::T_ScalarArrayOpExpr => {
            /* ScalarArrayOpExpr's result is boolean ... */
            coll = InvalidOid; /* ... so it has no collation */
        }
        NodeTag::T_BoolExpr => {
            /* BoolExpr's result is boolean ... */
            coll = InvalidOid; /* ... so it has no collation */
        }
        NodeTag::T_SubLink => {
            let sublink = expr as *const SubLink;

            if (*sublink).subLinkType == EXPR_SUBLINK
                || (*sublink).subLinkType == ARRAY_SUBLINK
            {
                /* get the collation of subselect's first target column */
                let qtree = (*sublink).subselect as *mut Query;
                let tent: *mut TargetEntry;

                if qtree.is_null() || !IsA!(qtree, T_Query) {
                    elog!(ERROR, "cannot get collation for untransformed sublink");
                }
                tent = linitial_node!(TargetEntry, T_TargetEntry, (*qtree).targetList);
                Assert!(!(*tent).resjunk);
                coll = exprCollation((*tent).expr as *const Node);
                /* collation doesn't change if it's converted to array */
            } else {
                /* otherwise, SubLink's result is RECORD or BOOLEAN */
                coll = InvalidOid; /* ... so it has no collation */
            }
        }
        NodeTag::T_SubPlan => {
            let subplan = expr as *const SubPlan;

            if (*subplan).subLinkType == EXPR_SUBLINK
                || (*subplan).subLinkType == ARRAY_SUBLINK
            {
                /* get the collation of subselect's first target column */
                coll = (*subplan).firstColCollation;
                /* collation doesn't change if it's converted to array */
            } else {
                /* otherwise, SubPlan's result is RECORD or BOOLEAN */
                coll = InvalidOid; /* ... so it has no collation */
            }
        }
        NodeTag::T_AlternativeSubPlan => {
            let asplan = expr as *const AlternativeSubPlan;

            /* subplans should all return the same thing */
            coll = exprCollation(linitial((*asplan).subplans) as *const Node);
        }
        NodeTag::T_FieldSelect => {
            coll = (*(expr as *const FieldSelect)).resultcollid;
        }
        NodeTag::T_FieldStore => {
            /* FieldStore's result is composite ... */
            coll = InvalidOid; /* ... so it has no collation */
        }
        NodeTag::T_RelabelType => {
            coll = (*(expr as *const RelabelType)).resultcollid;
        }
        NodeTag::T_CoerceViaIO => {
            coll = (*(expr as *const CoerceViaIO)).resultcollid;
        }
        NodeTag::T_ArrayCoerceExpr => {
            coll = (*(expr as *const ArrayCoerceExpr)).resultcollid;
        }
        NodeTag::T_ConvertRowtypeExpr => {
            /* ConvertRowtypeExpr's result is composite ... */
            coll = InvalidOid; /* ... so it has no collation */
        }
        NodeTag::T_CollateExpr => {
            coll = (*(expr as *const CollateExpr)).collOid;
        }
        NodeTag::T_CaseExpr => {
            coll = (*(expr as *const CaseExpr)).casecollid;
        }
        NodeTag::T_CaseTestExpr => {
            coll = (*(expr as *const CaseTestExpr)).collation;
        }
        NodeTag::T_ArrayExpr => {
            coll = (*(expr as *const ArrayExpr)).array_collid;
        }
        NodeTag::T_RowExpr => {
            /* RowExpr's result is composite ... */
            coll = InvalidOid; /* ... so it has no collation */
        }
        NodeTag::T_RowCompareExpr => {
            /* RowCompareExpr's result is boolean ... */
            coll = InvalidOid; /* ... so it has no collation */
        }
        NodeTag::T_CoalesceExpr => {
            coll = (*(expr as *const CoalesceExpr)).coalescecollid;
        }
        NodeTag::T_MinMaxExpr => {
            coll = (*(expr as *const MinMaxExpr)).minmaxcollid;
        }
        NodeTag::T_SQLValueFunction => {
            /* Returns either NAME or a non-collatable type */
            if (*(expr as *const SQLValueFunction)).r#type == NAMEOID {
                coll = C_COLLATION_OID;
            } else {
                coll = InvalidOid;
            }
        }
        NodeTag::T_XmlExpr => {
            /*
             * XMLSERIALIZE returns text from non-collatable inputs, so its
             * collation is always default.  The other cases return boolean or
             * XML, which are non-collatable.
             */
            if (*(expr as *const XmlExpr)).op == IS_XMLSERIALIZE {
                coll = DEFAULT_COLLATION_OID;
            } else {
                coll = InvalidOid;
            }
        }
        NodeTag::T_JsonValueExpr => {
            coll = exprCollation((*(expr as *const JsonValueExpr)).formatted_expr as *const Node);
        }
        NodeTag::T_JsonConstructorExpr => {
            let ctor = expr as *const JsonConstructorExpr;

            if !(*ctor).coercion.is_null() {
                coll = exprCollation((*ctor).coercion as *const Node);
            } else {
                coll = InvalidOid;
            }
        }
        NodeTag::T_JsonIsPredicate => {
            /* IS JSON's result is boolean ... */
            coll = InvalidOid; /* ... so it has no collation */
        }
        NodeTag::T_JsonExpr => {
            let jsexpr = expr as *const JsonExpr;

            coll = (*jsexpr).collation;
        }
        NodeTag::T_JsonBehavior => {
            let behavior = expr as *const JsonBehavior;

            if !(*behavior).expr.is_null() {
                coll = exprCollation((*behavior).expr);
            } else {
                coll = InvalidOid;
            }
        }
        NodeTag::T_NullTest => {
            /* NullTest's result is boolean ... */
            coll = InvalidOid; /* ... so it has no collation */
        }
        NodeTag::T_BooleanTest => {
            /* BooleanTest's result is boolean ... */
            coll = InvalidOid; /* ... so it has no collation */
        }
        NodeTag::T_CoerceToDomain => {
            coll = (*(expr as *const CoerceToDomain)).resultcollid;
        }
        NodeTag::T_CoerceToDomainValue => {
            coll = (*(expr as *const CoerceToDomainValue)).collation;
        }
        NodeTag::T_SetToDefault => {
            coll = (*(expr as *const SetToDefault)).collation;
        }
        NodeTag::T_CurrentOfExpr => {
            /* CurrentOfExpr's result is boolean ... */
            coll = InvalidOid; /* ... so it has no collation */
        }
        NodeTag::T_NextValueExpr => {
            /* NextValueExpr's result is an integer type ... */
            coll = InvalidOid; /* ... so it has no collation */
        }
        NodeTag::T_InferenceElem => {
            coll = exprCollation((*(expr as *const InferenceElem)).expr as *const Node);
        }
        NodeTag::T_ReturningExpr => {
            coll = exprCollation((*(expr as *const ReturningExpr)).retexpr as *const Node);
        }
        NodeTag::T_PlaceHolderVar => {
            coll = exprCollation((*(expr as *const PlaceHolderVar)).phexpr as *const Node);
        }
        _ => {
            elog!(ERROR, "unrecognized node type: {}", nodeTag(expr) as c_int);
            #[allow(unreachable_code)]
            {
                return InvalidOid; /* keep compiler quiet */
            }
        }
    }
    coll
}

/*
 *	exprInputCollation -
 *	  returns the Oid of the collation a function should use, if available.
 *
 * Result is InvalidOid if the node type doesn't store this information.
 */
pub unsafe fn exprInputCollation(expr: *const Node) -> Oid {
    let coll: Oid;

    if expr.is_null() {
        return InvalidOid;
    }

    match nodeTag(expr) {
        NodeTag::T_Aggref => {
            coll = (*(expr as *const Aggref)).inputcollid;
        }
        NodeTag::T_WindowFunc => {
            coll = (*(expr as *const WindowFunc)).inputcollid;
        }
        NodeTag::T_FuncExpr => {
            coll = (*(expr as *const FuncExpr)).inputcollid;
        }
        NodeTag::T_OpExpr => {
            coll = (*(expr as *const OpExpr)).inputcollid;
        }
        NodeTag::T_DistinctExpr => {
            coll = (*(expr as *const DistinctExpr)).inputcollid;
        }
        NodeTag::T_NullIfExpr => {
            coll = (*(expr as *const NullIfExpr)).inputcollid;
        }
        NodeTag::T_ScalarArrayOpExpr => {
            coll = (*(expr as *const ScalarArrayOpExpr)).inputcollid;
        }
        NodeTag::T_MinMaxExpr => {
            coll = (*(expr as *const MinMaxExpr)).inputcollid;
        }
        _ => {
            coll = InvalidOid;
        }
    }
    coll
}

/*
 *	exprSetCollation -
 *	  Assign collation information to an expression tree node.
 *
 * Note: since this is only used during parse analysis, we don't need to
 * worry about subplans, PlaceHolderVars, or ReturningExprs.
 */
pub unsafe fn exprSetCollation(expr: *mut Node, collation: Oid) {
    match nodeTag(expr) {
        NodeTag::T_Var => {
            (*(expr as *mut Var)).varcollid = collation;
        }
        NodeTag::T_Const => {
            (*(expr as *mut Const)).constcollid = collation;
        }
        NodeTag::T_Param => {
            (*(expr as *mut Param)).paramcollid = collation;
        }
        NodeTag::T_Aggref => {
            (*(expr as *mut Aggref)).aggcollid = collation;
        }
        NodeTag::T_GroupingFunc => {
            Assert!(!OidIsValid(collation));
        }
        NodeTag::T_WindowFunc => {
            (*(expr as *mut WindowFunc)).wincollid = collation;
        }
        NodeTag::T_MergeSupportFunc => {
            (*(expr as *mut MergeSupportFunc)).msfcollid = collation;
        }
        NodeTag::T_SubscriptingRef => {
            (*(expr as *mut SubscriptingRef)).refcollid = collation;
        }
        NodeTag::T_FuncExpr => {
            (*(expr as *mut FuncExpr)).funccollid = collation;
        }
        NodeTag::T_NamedArgExpr => {
            Assert!(collation == exprCollation((*(expr as *mut NamedArgExpr)).arg as *const Node));
        }
        NodeTag::T_OpExpr => {
            (*(expr as *mut OpExpr)).opcollid = collation;
        }
        NodeTag::T_DistinctExpr => {
            (*(expr as *mut DistinctExpr)).opcollid = collation;
        }
        NodeTag::T_NullIfExpr => {
            (*(expr as *mut NullIfExpr)).opcollid = collation;
        }
        NodeTag::T_ScalarArrayOpExpr => {
            /* ScalarArrayOpExpr's result is boolean ... */
            Assert!(!OidIsValid(collation)); /* ... so never set a collation */
        }
        NodeTag::T_BoolExpr => {
            /* BoolExpr's result is boolean ... */
            Assert!(!OidIsValid(collation)); /* ... so never set a collation */
        }
        NodeTag::T_SubLink => {
            /* USE_ASSERT_CHECKING block */
            #[cfg(debug_assertions)]
            {
                let sublink = expr as *mut SubLink;

                if (*sublink).subLinkType == EXPR_SUBLINK
                    || (*sublink).subLinkType == ARRAY_SUBLINK
                {
                    /* get the collation of subselect's first target column */
                    let qtree = (*sublink).subselect as *mut Query;
                    let tent: *mut TargetEntry;

                    if qtree.is_null() || !IsA!(qtree, T_Query) {
                        elog!(ERROR, "cannot set collation for untransformed sublink");
                    }
                    tent = linitial_node!(TargetEntry, T_TargetEntry, (*qtree).targetList);
                    Assert!(!(*tent).resjunk);
                    Assert!(collation == exprCollation((*tent).expr as *const Node));
                } else {
                    /* otherwise, result is RECORD or BOOLEAN */
                    Assert!(!OidIsValid(collation));
                }
            }
        }
        NodeTag::T_FieldSelect => {
            (*(expr as *mut FieldSelect)).resultcollid = collation;
        }
        NodeTag::T_FieldStore => {
            /* FieldStore's result is composite ... */
            Assert!(!OidIsValid(collation)); /* ... so never set a collation */
        }
        NodeTag::T_RelabelType => {
            (*(expr as *mut RelabelType)).resultcollid = collation;
        }
        NodeTag::T_CoerceViaIO => {
            (*(expr as *mut CoerceViaIO)).resultcollid = collation;
        }
        NodeTag::T_ArrayCoerceExpr => {
            (*(expr as *mut ArrayCoerceExpr)).resultcollid = collation;
        }
        NodeTag::T_ConvertRowtypeExpr => {
            /* ConvertRowtypeExpr's result is composite ... */
            Assert!(!OidIsValid(collation)); /* ... so never set a collation */
        }
        NodeTag::T_CaseExpr => {
            (*(expr as *mut CaseExpr)).casecollid = collation;
        }
        NodeTag::T_ArrayExpr => {
            (*(expr as *mut ArrayExpr)).array_collid = collation;
        }
        NodeTag::T_RowExpr => {
            /* RowExpr's result is composite ... */
            Assert!(!OidIsValid(collation)); /* ... so never set a collation */
        }
        NodeTag::T_RowCompareExpr => {
            /* RowCompareExpr's result is boolean ... */
            Assert!(!OidIsValid(collation)); /* ... so never set a collation */
        }
        NodeTag::T_CoalesceExpr => {
            (*(expr as *mut CoalesceExpr)).coalescecollid = collation;
        }
        NodeTag::T_MinMaxExpr => {
            (*(expr as *mut MinMaxExpr)).minmaxcollid = collation;
        }
        NodeTag::T_SQLValueFunction => {
            Assert!(if (*(expr as *mut SQLValueFunction)).r#type == NAMEOID {
                collation == C_COLLATION_OID
            } else {
                collation == InvalidOid
            });
        }
        NodeTag::T_XmlExpr => {
            Assert!(if (*(expr as *mut XmlExpr)).op == IS_XMLSERIALIZE {
                collation == DEFAULT_COLLATION_OID
            } else {
                collation == InvalidOid
            });
        }
        NodeTag::T_JsonValueExpr => {
            exprSetCollation(
                (*(expr as *mut JsonValueExpr)).formatted_expr as *mut Node,
                collation,
            );
        }
        NodeTag::T_JsonConstructorExpr => {
            let ctor = expr as *mut JsonConstructorExpr;

            if !(*ctor).coercion.is_null() {
                exprSetCollation((*ctor).coercion as *mut Node, collation);
            } else {
                Assert!(!OidIsValid(collation)); /* result is always a json[b] type */
            }
        }
        NodeTag::T_JsonIsPredicate => {
            Assert!(!OidIsValid(collation)); /* result is always boolean */
        }
        NodeTag::T_JsonExpr => {
            let jexpr = expr as *mut JsonExpr;

            (*jexpr).collation = collation;
        }
        NodeTag::T_JsonBehavior => {
            Assert!(
                (*(expr as *mut JsonBehavior)).expr.is_null()
                    || exprCollation((*(expr as *mut JsonBehavior)).expr) == collation
            );
        }
        NodeTag::T_NullTest => {
            /* NullTest's result is boolean ... */
            Assert!(!OidIsValid(collation)); /* ... so never set a collation */
        }
        NodeTag::T_BooleanTest => {
            /* BooleanTest's result is boolean ... */
            Assert!(!OidIsValid(collation)); /* ... so never set a collation */
        }
        NodeTag::T_CoerceToDomain => {
            (*(expr as *mut CoerceToDomain)).resultcollid = collation;
        }
        NodeTag::T_CoerceToDomainValue => {
            (*(expr as *mut CoerceToDomainValue)).collation = collation;
        }
        NodeTag::T_SetToDefault => {
            (*(expr as *mut SetToDefault)).collation = collation;
        }
        NodeTag::T_CurrentOfExpr => {
            /* CurrentOfExpr's result is boolean ... */
            Assert!(!OidIsValid(collation)); /* ... so never set a collation */
        }
        NodeTag::T_NextValueExpr => {
            /* NextValueExpr's result is an integer type ... */
            Assert!(!OidIsValid(collation)); /* ... so never set a collation */
        }
        _ => {
            elog!(ERROR, "unrecognized node type: {}", nodeTag(expr) as c_int);
        }
    }
}

/*
 *	exprSetInputCollation -
 *	  Assign input-collation information to an expression tree node.
 *
 * This is a no-op for node types that don't store their input collation.
 * Note we omit RowCompareExpr, which needs special treatment since it
 * contains multiple input collation OIDs.
 */
pub unsafe fn exprSetInputCollation(expr: *mut Node, inputcollation: Oid) {
    match nodeTag(expr) {
        NodeTag::T_Aggref => {
            (*(expr as *mut Aggref)).inputcollid = inputcollation;
        }
        NodeTag::T_WindowFunc => {
            (*(expr as *mut WindowFunc)).inputcollid = inputcollation;
        }
        NodeTag::T_FuncExpr => {
            (*(expr as *mut FuncExpr)).inputcollid = inputcollation;
        }
        NodeTag::T_OpExpr => {
            (*(expr as *mut OpExpr)).inputcollid = inputcollation;
        }
        NodeTag::T_DistinctExpr => {
            (*(expr as *mut DistinctExpr)).inputcollid = inputcollation;
        }
        NodeTag::T_NullIfExpr => {
            (*(expr as *mut NullIfExpr)).inputcollid = inputcollation;
        }
        NodeTag::T_ScalarArrayOpExpr => {
            (*(expr as *mut ScalarArrayOpExpr)).inputcollid = inputcollation;
        }
        NodeTag::T_MinMaxExpr => {
            (*(expr as *mut MinMaxExpr)).inputcollid = inputcollation;
        }
        _ => {}
    }
}

/*
 *	exprLocation -
 *	  returns the parse location of an expression tree, for error reports
 *
 * -1 is returned if the location can't be determined.
 *
 * For expressions larger than a single token, the intent here is to
 * return the location of the expression's leftmost token, not necessarily
 * the topmost Node's location field.  For example, an OpExpr's location
 * field will point at the operator name, but if it is not a prefix operator
 * then we should return the location of the left-hand operand instead.
 * The reason is that we want to reference the entire expression not just
 * that operator, and pointing to its start seems to be the most natural way.
 *
 * The location is not perfect --- for example, since the grammar doesn't
 * explicitly represent parentheses in the parsetree, given something that
 * had been written "(a + b) * c" we are going to point at "a" not "(".
 * But it should be plenty good enough for error reporting purposes.
 *
 * You might think that this code is overly general, for instance why check
 * the operands of a FuncExpr node, when the function name can be expected
 * to be to the left of them?  There are a couple of reasons.  The grammar
 * sometimes builds expressions that aren't quite what the user wrote;
 * for instance x IS NOT BETWEEN ... becomes a NOT-expression whose keyword
 * pointer is to the right of its leftmost argument.  Also, nodes that were
 * inserted implicitly by parse analysis (such as FuncExprs for implicit
 * coercions) will have location -1, and so we can have odd combinations of
 * known and unknown locations in a tree.
 */
pub unsafe fn exprLocation(expr: *const Node) -> c_int {
    let loc: c_int;

    if expr.is_null() {
        return -1;
    }
    match nodeTag(expr) {
        NodeTag::T_RangeVar => {
            loc = (*(expr as *const RangeVar)).location;
        }
        NodeTag::T_TableFunc => {
            loc = (*(expr as *const TableFunc)).location;
        }
        NodeTag::T_Var => {
            loc = (*(expr as *const Var)).location;
        }
        NodeTag::T_Const => {
            loc = (*(expr as *const Const)).location;
        }
        NodeTag::T_Param => {
            loc = (*(expr as *const Param)).location;
        }
        NodeTag::T_Aggref => {
            /* function name should always be the first thing */
            loc = (*(expr as *const Aggref)).location;
        }
        NodeTag::T_GroupingFunc => {
            loc = (*(expr as *const GroupingFunc)).location;
        }
        NodeTag::T_WindowFunc => {
            /* function name should always be the first thing */
            loc = (*(expr as *const WindowFunc)).location;
        }
        NodeTag::T_MergeSupportFunc => {
            loc = (*(expr as *const MergeSupportFunc)).location;
        }
        NodeTag::T_SubscriptingRef => {
            /* just use container argument's location */
            loc = exprLocation((*(expr as *const SubscriptingRef)).refexpr as *const Node);
        }
        NodeTag::T_FuncExpr => {
            let fexpr = expr as *const FuncExpr;

            /* consider both function name and leftmost arg */
            loc = leftmostLoc((*fexpr).location, exprLocation((*fexpr).args as *const Node));
        }
        NodeTag::T_NamedArgExpr => {
            let na = expr as *const NamedArgExpr;

            /* consider both argument name and value */
            loc = leftmostLoc((*na).location, exprLocation((*na).arg as *const Node));
        }
        NodeTag::T_OpExpr | NodeTag::T_DistinctExpr | NodeTag::T_NullIfExpr => {
            /* T_DistinctExpr, T_NullIfExpr struct-equivalent to OpExpr */
            let opexpr = expr as *const OpExpr;

            /* consider both operator name and leftmost arg */
            loc = leftmostLoc((*opexpr).location, exprLocation((*opexpr).args as *const Node));
        }
        NodeTag::T_ScalarArrayOpExpr => {
            let saopexpr = expr as *const ScalarArrayOpExpr;

            /* consider both operator name and leftmost arg */
            loc = leftmostLoc(
                (*saopexpr).location,
                exprLocation((*saopexpr).args as *const Node),
            );
        }
        NodeTag::T_BoolExpr => {
            let bexpr = expr as *const BoolExpr;

            /*
             * Same as above, to handle either NOT or AND/OR.  We can't
             * special-case NOT because of the way that it's used for
             * things like IS NOT BETWEEN.
             */
            loc = leftmostLoc((*bexpr).location, exprLocation((*bexpr).args as *const Node));
        }
        NodeTag::T_SubLink => {
            let sublink = expr as *const SubLink;

            /* check the testexpr, if any, and the operator/keyword */
            loc = leftmostLoc(exprLocation((*sublink).testexpr), (*sublink).location);
        }
        NodeTag::T_FieldSelect => {
            /* just use argument's location */
            loc = exprLocation((*(expr as *const FieldSelect)).arg as *const Node);
        }
        NodeTag::T_FieldStore => {
            /* just use argument's location */
            loc = exprLocation((*(expr as *const FieldStore)).arg as *const Node);
        }
        NodeTag::T_RelabelType => {
            let rexpr = expr as *const RelabelType;

            /* Much as above */
            loc = leftmostLoc((*rexpr).location, exprLocation((*rexpr).arg as *const Node));
        }
        NodeTag::T_CoerceViaIO => {
            let cexpr = expr as *const CoerceViaIO;

            /* Much as above */
            loc = leftmostLoc((*cexpr).location, exprLocation((*cexpr).arg as *const Node));
        }
        NodeTag::T_ArrayCoerceExpr => {
            let cexpr = expr as *const ArrayCoerceExpr;

            /* Much as above */
            loc = leftmostLoc((*cexpr).location, exprLocation((*cexpr).arg as *const Node));
        }
        NodeTag::T_ConvertRowtypeExpr => {
            let cexpr = expr as *const ConvertRowtypeExpr;

            /* Much as above */
            loc = leftmostLoc((*cexpr).location, exprLocation((*cexpr).arg as *const Node));
        }
        NodeTag::T_CollateExpr => {
            /* just use argument's location */
            loc = exprLocation((*(expr as *const CollateExpr)).arg as *const Node);
        }
        NodeTag::T_CaseExpr => {
            /* CASE keyword should always be the first thing */
            loc = (*(expr as *const CaseExpr)).location;
        }
        NodeTag::T_CaseWhen => {
            /* WHEN keyword should always be the first thing */
            loc = (*(expr as *const CaseWhen)).location;
        }
        NodeTag::T_ArrayExpr => {
            /* the location points at ARRAY or [, which must be leftmost */
            loc = (*(expr as *const ArrayExpr)).location;
        }
        NodeTag::T_RowExpr => {
            /* the location points at ROW or (, which must be leftmost */
            loc = (*(expr as *const RowExpr)).location;
        }
        NodeTag::T_RowCompareExpr => {
            /* just use leftmost argument's location */
            loc = exprLocation((*(expr as *const RowCompareExpr)).largs as *const Node);
        }
        NodeTag::T_CoalesceExpr => {
            /* COALESCE keyword should always be the first thing */
            loc = (*(expr as *const CoalesceExpr)).location;
        }
        NodeTag::T_MinMaxExpr => {
            /* GREATEST/LEAST keyword should always be the first thing */
            loc = (*(expr as *const MinMaxExpr)).location;
        }
        NodeTag::T_SQLValueFunction => {
            /* function keyword should always be the first thing */
            loc = (*(expr as *const SQLValueFunction)).location;
        }
        NodeTag::T_XmlExpr => {
            let xexpr = expr as *const XmlExpr;

            /* consider both function name and leftmost arg */
            loc = leftmostLoc((*xexpr).location, exprLocation((*xexpr).args as *const Node));
        }
        NodeTag::T_JsonFormat => {
            loc = (*(expr as *const JsonFormat)).location;
        }
        NodeTag::T_JsonValueExpr => {
            loc = exprLocation((*(expr as *const JsonValueExpr)).raw_expr as *const Node);
        }
        NodeTag::T_JsonConstructorExpr => {
            loc = (*(expr as *const JsonConstructorExpr)).location;
        }
        NodeTag::T_JsonIsPredicate => {
            loc = (*(expr as *const JsonIsPredicate)).location;
        }
        NodeTag::T_JsonExpr => {
            let jsexpr = expr as *const JsonExpr;

            /* consider both function name and leftmost arg */
            loc = leftmostLoc((*jsexpr).location, exprLocation((*jsexpr).formatted_expr));
        }
        NodeTag::T_JsonBehavior => {
            loc = exprLocation((*(expr as *const JsonBehavior)).expr);
        }
        NodeTag::T_NullTest => {
            let nexpr = expr as *const NullTest;

            /* Much as above */
            loc = leftmostLoc((*nexpr).location, exprLocation((*nexpr).arg as *const Node));
        }
        NodeTag::T_BooleanTest => {
            let bexpr = expr as *const BooleanTest;

            /* Much as above */
            loc = leftmostLoc((*bexpr).location, exprLocation((*bexpr).arg as *const Node));
        }
        NodeTag::T_CoerceToDomain => {
            let cexpr = expr as *const CoerceToDomain;

            /* Much as above */
            loc = leftmostLoc((*cexpr).location, exprLocation((*cexpr).arg as *const Node));
        }
        NodeTag::T_CoerceToDomainValue => {
            loc = (*(expr as *const CoerceToDomainValue)).location;
        }
        NodeTag::T_SetToDefault => {
            loc = (*(expr as *const SetToDefault)).location;
        }
        NodeTag::T_ReturningExpr => {
            loc = exprLocation((*(expr as *const ReturningExpr)).retexpr as *const Node);
        }
        NodeTag::T_TargetEntry => {
            /* just use argument's location */
            loc = exprLocation((*(expr as *const TargetEntry)).expr as *const Node);
        }
        NodeTag::T_IntoClause => {
            /* use the contained RangeVar's location --- close enough */
            loc = exprLocation((*(expr as *const IntoClause)).rel as *const Node);
        }
        NodeTag::T_List => {
            /* report location of first list member that has a location */
            let mut l: c_int = -1; /* just to suppress compiler warning */
            foreach!(lc, expr as *const List, {
                l = exprLocation(lfirst(current_cell!(lc)) as *const Node);
                if l >= 0 {
                    break;
                }
            });
            loc = l;
        }
        NodeTag::T_A_Expr => {
            let aexpr = expr as *const A_Expr;

            /* use leftmost of operator or left operand (if any) */
            /* we assume right operand can't be to left of operator */
            loc = leftmostLoc((*aexpr).location, exprLocation((*aexpr).lexpr));
        }
        NodeTag::T_ColumnRef => {
            loc = (*(expr as *const ColumnRef)).location;
        }
        NodeTag::T_ParamRef => {
            loc = (*(expr as *const ParamRef)).location;
        }
        NodeTag::T_A_Const => {
            loc = (*(expr as *const A_Const)).location;
        }
        NodeTag::T_FuncCall => {
            let fc = expr as *const FuncCall;

            /* consider both function name and leftmost arg */
            /* (we assume any ORDER BY nodes must be to right of name) */
            loc = leftmostLoc((*fc).location, exprLocation((*fc).args as *const Node));
        }
        NodeTag::T_A_ArrayExpr => {
            /* the location points at ARRAY or [, which must be leftmost */
            loc = (*(expr as *const A_ArrayExpr)).location;
        }
        NodeTag::T_ResTarget => {
            /* we need not examine the contained expression (if any) */
            loc = (*(expr as *const ResTarget)).location;
        }
        NodeTag::T_MultiAssignRef => {
            loc = exprLocation((*(expr as *const MultiAssignRef)).source);
        }
        NodeTag::T_TypeCast => {
            let tc = expr as *const TypeCast;

            /*
             * This could represent CAST(), ::, or TypeName 'literal', so
             * any of the components might be leftmost.
             */
            let mut l = exprLocation((*tc).arg);
            l = leftmostLoc(l, (*(*tc).typeName).location);
            l = leftmostLoc(l, (*tc).location);
            loc = l;
        }
        NodeTag::T_CollateClause => {
            /* just use argument's location */
            loc = exprLocation((*(expr as *const CollateClause)).arg);
        }
        NodeTag::T_SortBy => {
            /* just use argument's location (ignore operator, if any) */
            loc = exprLocation((*(expr as *const SortBy)).node);
        }
        NodeTag::T_WindowDef => {
            loc = (*(expr as *const WindowDef)).location;
        }
        NodeTag::T_RangeTableSample => {
            loc = (*(expr as *const RangeTableSample)).location;
        }
        NodeTag::T_TypeName => {
            loc = (*(expr as *const TypeName)).location;
        }
        NodeTag::T_ColumnDef => {
            loc = (*(expr as *const ColumnDef)).location;
        }
        NodeTag::T_Constraint => {
            loc = (*(expr as *const Constraint)).location;
        }
        NodeTag::T_FunctionParameter => {
            loc = (*(expr as *const FunctionParameter)).location;
        }
        NodeTag::T_XmlSerialize => {
            /* XMLSERIALIZE keyword should always be the first thing */
            loc = (*(expr as *const XmlSerialize)).location;
        }
        NodeTag::T_GroupingSet => {
            loc = (*(expr as *const GroupingSet)).location;
        }
        NodeTag::T_WithClause => {
            loc = (*(expr as *const WithClause)).location;
        }
        NodeTag::T_InferClause => {
            loc = (*(expr as *const InferClause)).location;
        }
        NodeTag::T_OnConflictClause => {
            loc = (*(expr as *const OnConflictClause)).location;
        }
        NodeTag::T_CTESearchClause => {
            loc = (*(expr as *const CTESearchClause)).location;
        }
        NodeTag::T_CTECycleClause => {
            loc = (*(expr as *const CTECycleClause)).location;
        }
        NodeTag::T_CommonTableExpr => {
            loc = (*(expr as *const CommonTableExpr)).location;
        }
        NodeTag::T_JsonKeyValue => {
            /* just use the key's location */
            loc = exprLocation((*(expr as *const JsonKeyValue)).key as *const Node);
        }
        NodeTag::T_JsonObjectConstructor => {
            loc = (*(expr as *const JsonObjectConstructor)).location;
        }
        NodeTag::T_JsonArrayConstructor => {
            loc = (*(expr as *const JsonArrayConstructor)).location;
        }
        NodeTag::T_JsonArrayQueryConstructor => {
            loc = (*(expr as *const JsonArrayQueryConstructor)).location;
        }
        NodeTag::T_JsonAggConstructor => {
            loc = (*(expr as *const JsonAggConstructor)).location;
        }
        NodeTag::T_JsonObjectAgg => {
            loc = exprLocation((*(expr as *const JsonObjectAgg)).constructor as *const Node);
        }
        NodeTag::T_JsonArrayAgg => {
            loc = exprLocation((*(expr as *const JsonArrayAgg)).constructor as *const Node);
        }
        NodeTag::T_PlaceHolderVar => {
            /* just use argument's location */
            loc = exprLocation((*(expr as *const PlaceHolderVar)).phexpr as *const Node);
        }
        NodeTag::T_InferenceElem => {
            /* just use nested expr's location */
            loc = exprLocation((*(expr as *const InferenceElem)).expr as *const Node);
        }
        NodeTag::T_PartitionElem => {
            loc = (*(expr as *const PartitionElem)).location;
        }
        NodeTag::T_PartitionSpec => {
            loc = (*(expr as *const PartitionSpec)).location;
        }
        NodeTag::T_PartitionBoundSpec => {
            loc = (*(expr as *const PartitionBoundSpec)).location;
        }
        NodeTag::T_PartitionRangeDatum => {
            loc = (*(expr as *const PartitionRangeDatum)).location;
        }
        _ => {
            /* for any other node type it's just unknown... */
            loc = -1;
        }
    }
    loc
}

/*
 * leftmostLoc - support for exprLocation
 *
 * Take the minimum of two parse location values, but ignore unknowns
 */
fn leftmostLoc(loc1: c_int, loc2: c_int) -> c_int {
    if loc1 < 0 {
        loc2
    } else if loc2 < 0 {
        loc1
    } else {
        Min(loc1, loc2)
    }
}

/*
 * fix_opfuncids
 *	  Calculate opfuncid field from opno for each OpExpr node in given tree.
 *	  The given tree can be anything expression_tree_walker handles.
 *
 * The argument is modified in-place.  (This is OK since we'd want the
 * same change for any node, even if it gets visited more than once due to
 * shared structure.)
 */
pub unsafe fn fix_opfuncids(node: *mut Node) {
    /* This tree walk requires no special setup, so away we go... */
    fix_opfuncids_walker(node, null_mut());
}

unsafe fn fix_opfuncids_walker(node: *mut Node, context: *mut c_void) -> bool {
    if node.is_null() {
        return false;
    }
    if IsA!(node, T_OpExpr) {
        set_opfuncid(node as *mut OpExpr);
    } else if IsA!(node, T_DistinctExpr) {
        set_opfuncid(node as *mut OpExpr); /* rely on struct equivalence */
    } else if IsA!(node, T_NullIfExpr) {
        set_opfuncid(node as *mut OpExpr); /* rely on struct equivalence */
    } else if IsA!(node, T_ScalarArrayOpExpr) {
        set_sa_opfuncid(node as *mut ScalarArrayOpExpr);
    }
    expression_tree_walker(node, Some(fix_opfuncids_walker), context)
}

/*
 * set_opfuncid
 *		Set the opfuncid (procedure OID) in an OpExpr node,
 *		if it hasn't been set already.
 *
 * Because of struct equivalence, this can also be used for
 * DistinctExpr and NullIfExpr nodes.
 */
pub unsafe fn set_opfuncid(opexpr: *mut OpExpr) {
    if (*opexpr).opfuncid == InvalidOid {
        (*opexpr).opfuncid = get_opcode((*opexpr).opno);
    }
}

/*
 * set_sa_opfuncid
 *		As above, for ScalarArrayOpExpr nodes.
 */
pub unsafe fn set_sa_opfuncid(opexpr: *mut ScalarArrayOpExpr) {
    if (*opexpr).opfuncid == InvalidOid {
        (*opexpr).opfuncid = get_opcode((*opexpr).opno);
    }
}

/*
 *	check_functions_in_node -
 *	  apply checker() to each function OID contained in given expression node
 *
 * Returns true if the checker() function does; for nodes representing more
 * than one function call, returns true if the checker() function does so
 * for any of those functions.  Returns false if node does not invoke any
 * SQL-visible function.  Caller must not pass node == NULL.
 *
 * This function examines only the given node; it does not recurse into any
 * sub-expressions.  Callers typically prefer to keep control of the recursion
 * for themselves, in case additional checks should be made, or because they
 * have special rules about which parts of the tree need to be visited.
 *
 * Note: we ignore MinMaxExpr, SQLValueFunction, XmlExpr, CoerceToDomain,
 * and NextValueExpr nodes, because they do not contain SQL function OIDs.
 * However, they can invoke SQL-visible functions, so callers should take
 * thought about how to treat them.
 */
pub unsafe fn check_functions_in_node(
    node: *mut Node,
    checker: check_function_callback,
    context: *mut c_void,
) -> bool {
    let checker = checker.unwrap();
    match nodeTag(node) {
        NodeTag::T_Aggref => {
            let expr = node as *mut Aggref;

            if checker((*expr).aggfnoid, context) {
                return true;
            }
        }
        NodeTag::T_WindowFunc => {
            let expr = node as *mut WindowFunc;

            if checker((*expr).winfnoid, context) {
                return true;
            }
        }
        NodeTag::T_FuncExpr => {
            let expr = node as *mut FuncExpr;

            if checker((*expr).funcid, context) {
                return true;
            }
        }
        NodeTag::T_OpExpr | NodeTag::T_DistinctExpr | NodeTag::T_NullIfExpr => {
            /* T_DistinctExpr, T_NullIfExpr struct-equivalent to OpExpr */
            let expr = node as *mut OpExpr;

            /* Set opfuncid if it wasn't set already */
            set_opfuncid(expr);
            if checker((*expr).opfuncid, context) {
                return true;
            }
        }
        NodeTag::T_ScalarArrayOpExpr => {
            let expr = node as *mut ScalarArrayOpExpr;

            set_sa_opfuncid(expr);
            if checker((*expr).opfuncid, context) {
                return true;
            }
        }
        NodeTag::T_CoerceViaIO => {
            let expr = node as *mut CoerceViaIO;
            let mut iofunc: Oid = 0;
            let mut typioparam: Oid = 0;
            let mut typisvarlena: bool = false;

            /* check the result type's input function */
            getTypeInputInfo((*expr).resulttype, &mut iofunc, &mut typioparam);
            if checker(iofunc, context) {
                return true;
            }
            /* check the input type's output function */
            getTypeOutputInfo(
                exprType((*expr).arg as *const Node),
                &mut iofunc,
                &mut typisvarlena,
            );
            if checker(iofunc, context) {
                return true;
            }
        }
        NodeTag::T_RowCompareExpr => {
            let rcexpr = node as *mut RowCompareExpr;

            foreach!(opid, (*rcexpr).opnos, {
                let opfuncid: Oid = get_opcode(lfirst_oid(current_cell!(opid)));

                if checker(opfuncid, context) {
                    return true;
                }
            });
        }
        _ => {}
    }
    false
}

/*
 * Standard expression-tree walking support
 *
 * We used to have near-duplicate code in many different routines that
 * understood how to recurse through an expression node tree.  That was
 * a pain to maintain, and we frequently had bugs due to some particular
 * routine neglecting to support a particular node type.  In most cases,
 * these routines only actually care about certain node types, and don't
 * care about other types except insofar as they have to recurse through
 * non-primitive node types.  Therefore, we now provide generic tree-walking
 * logic to consolidate the redundant "boilerplate" code.  There are
 * two versions: expression_tree_walker() and expression_tree_mutator().
 */

/*
 * expression_tree_walker() is designed to support routines that traverse
 * a tree in a read-only fashion (although it will also work for routines
 * that modify nodes in-place but never add/delete/replace nodes).
 * A walker routine should look like this:
 *
 * bool my_walker (Node *node, my_struct *context)
 * {
 *		if (node == NULL)
 *			return false;
 *		// check for nodes that special work is required for, eg:
 *		if (IsA(node, Var))
 *		{
 *			... do special actions for Var nodes
 *		}
 *		else if (IsA(node, ...))
 *		{
 *			... do special actions for other node types
 *		}
 *		// for any node type not specially processed, do:
 *		return expression_tree_walker(node, my_walker, context);
 * }
 *
 * The "context" argument points to a struct that holds whatever context
 * information the walker routine needs --- it can be used to return data
 * gathered by the walker, too.  This argument is not touched by
 * expression_tree_walker, but it is passed down to recursive sub-invocations
 * of my_walker.  The tree walk is started from a setup routine that
 * fills in the appropriate context struct, calls my_walker with the top-level
 * node of the tree, and then examines the results.
 *
 * The walker routine should return "false" to continue the tree walk, or
 * "true" to abort the walk and immediately return "true" to the top-level
 * caller.  This can be used to short-circuit the traversal if the walker
 * has found what it came for.  "false" is returned to the top-level caller
 * iff no invocation of the walker returned "true".
 *
 * The node types handled by expression_tree_walker include all those
 * normally found in target lists and qualifier clauses during the planning
 * stage.  In particular, it handles List nodes since a cnf-ified qual clause
 * will have List structure at the top level, and it handles TargetEntry nodes
 * so that a scan of a target list can be handled without additional code.
 * Also, RangeTblRef, FromExpr, JoinExpr, and SetOperationStmt nodes are
 * handled, so that query jointrees and setOperation trees can be processed
 * without additional code.
 *
 * expression_tree_walker will handle SubLink nodes by recursing normally
 * into the "testexpr" subtree (which is an expression belonging to the outer
 * plan).  It will also call the walker on the sub-Query node; however, when
 * expression_tree_walker itself is called on a Query node, it does nothing
 * and returns "false".  The net effect is that unless the walker does
 * something special at a Query node, sub-selects will not be visited during
 * an expression tree walk. This is exactly the behavior wanted in many cases
 * --- and for those walkers that do want to recurse into sub-selects, special
 * behavior is typically needed anyway at the entry to a sub-select (such as
 * incrementing a depth counter). A walker that wants to examine sub-selects
 * should include code along the lines of:
 *
 *		if (IsA(node, Query))
 *		{
 *			adjust context for subquery;
 *			result = query_tree_walker((Query *) node, my_walker, context,
 *									   0); // adjust flags as needed
 *			restore context if needed;
 *			return result;
 *		}
 *
 * query_tree_walker is a convenience routine (see below) that calls the
 * walker on all the expression subtrees of the given Query node.
 *
 * expression_tree_walker will handle SubPlan nodes by recursing normally
 * into the "testexpr" and the "args" list (which are expressions belonging to
 * the outer plan).  It will not touch the completed subplan, however.  Since
 * there is no link to the original Query, it is not possible to recurse into
 * subselects of an already-planned expression tree.  This is OK for current
 * uses, but may need to be revisited in future.
 */
#[no_mangle]
pub unsafe fn expression_tree_walker_impl(
    node: *mut Node,
    walker: tree_walker_callback,
    context: *mut c_void,
) -> bool {
    /*
     * The walker has already visited the current node, and so we need only
     * recurse into any sub-nodes it has.
     *
     * We assume that the walker is not interested in List nodes per se, so
     * when we expect a List we just recurse directly to self without
     * bothering to call the walker.
     */
    let w = walker.unwrap();

    /* WALK(n) = walker((Node *) (n), context) */
    macro_rules! WALK {
        ($n:expr) => {
            w($n as *mut Node, context)
        };
    }
    /* LIST_WALK(l) = expression_tree_walker_impl((Node *) (l), walker, context) */
    macro_rules! LIST_WALK {
        ($l:expr) => {
            expression_tree_walker_impl($l as *mut Node, walker, context)
        };
    }

    if node.is_null() {
        return false;
    }

    /* Guard against stack overflow due to overly complex expressions */
    check_stack_depth();

    match nodeTag(node) {
        NodeTag::T_Var
        | NodeTag::T_Const
        | NodeTag::T_Param
        | NodeTag::T_CaseTestExpr
        | NodeTag::T_SQLValueFunction
        | NodeTag::T_CoerceToDomainValue
        | NodeTag::T_SetToDefault
        | NodeTag::T_CurrentOfExpr
        | NodeTag::T_NextValueExpr
        | NodeTag::T_RangeTblRef
        | NodeTag::T_SortGroupClause
        | NodeTag::T_CTESearchClause
        | NodeTag::T_MergeSupportFunc => {
            /* primitive node types with no expression subnodes */
        }
        NodeTag::T_WithCheckOption => {
            return WALK!((*(node as *mut WithCheckOption)).qual);
        }
        NodeTag::T_Aggref => {
            let expr = node as *mut Aggref;

            /* recurse directly on Lists */
            if LIST_WALK!((*expr).aggdirectargs) {
                return true;
            }
            if LIST_WALK!((*expr).args) {
                return true;
            }
            if LIST_WALK!((*expr).aggorder) {
                return true;
            }
            if LIST_WALK!((*expr).aggdistinct) {
                return true;
            }
            if WALK!((*expr).aggfilter) {
                return true;
            }
        }
        NodeTag::T_GroupingFunc => {
            let grouping = node as *mut GroupingFunc;

            if LIST_WALK!((*grouping).args) {
                return true;
            }
        }
        NodeTag::T_WindowFunc => {
            let expr = node as *mut WindowFunc;

            /* recurse directly on List */
            if LIST_WALK!((*expr).args) {
                return true;
            }
            if WALK!((*expr).aggfilter) {
                return true;
            }
            if WALK!((*expr).runCondition) {
                return true;
            }
        }
        NodeTag::T_WindowFuncRunCondition => {
            let expr = node as *mut WindowFuncRunCondition;

            if WALK!((*expr).arg) {
                return true;
            }
        }
        NodeTag::T_SubscriptingRef => {
            let sbsref = node as *mut SubscriptingRef;

            /* recurse directly for upper/lower container index lists */
            if LIST_WALK!((*sbsref).refupperindexpr) {
                return true;
            }
            if LIST_WALK!((*sbsref).reflowerindexpr) {
                return true;
            }
            /* walker must see the refexpr and refassgnexpr, however */
            if WALK!((*sbsref).refexpr) {
                return true;
            }

            if WALK!((*sbsref).refassgnexpr) {
                return true;
            }
        }
        NodeTag::T_FuncExpr => {
            let expr = node as *mut FuncExpr;

            if LIST_WALK!((*expr).args) {
                return true;
            }
        }
        NodeTag::T_NamedArgExpr => {
            return WALK!((*(node as *mut NamedArgExpr)).arg);
        }
        NodeTag::T_OpExpr | NodeTag::T_DistinctExpr | NodeTag::T_NullIfExpr => {
            /* T_DistinctExpr, T_NullIfExpr struct-equivalent to OpExpr */
            let expr = node as *mut OpExpr;

            if LIST_WALK!((*expr).args) {
                return true;
            }
        }
        NodeTag::T_ScalarArrayOpExpr => {
            let expr = node as *mut ScalarArrayOpExpr;

            if LIST_WALK!((*expr).args) {
                return true;
            }
        }
        NodeTag::T_BoolExpr => {
            let expr = node as *mut BoolExpr;

            if LIST_WALK!((*expr).args) {
                return true;
            }
        }
        NodeTag::T_SubLink => {
            let sublink = node as *mut SubLink;

            if WALK!((*sublink).testexpr) {
                return true;
            }

            /*
             * Also invoke the walker on the sublink's Query node, so it
             * can recurse into the sub-query if it wants to.
             */
            return WALK!((*sublink).subselect);
        }
        NodeTag::T_SubPlan => {
            let subplan = node as *mut SubPlan;

            /* recurse into the testexpr, but not into the Plan */
            if WALK!((*subplan).testexpr) {
                return true;
            }
            /* also examine args list */
            if LIST_WALK!((*subplan).args) {
                return true;
            }
        }
        NodeTag::T_AlternativeSubPlan => {
            return LIST_WALK!((*(node as *mut AlternativeSubPlan)).subplans);
        }
        NodeTag::T_FieldSelect => {
            return WALK!((*(node as *mut FieldSelect)).arg);
        }
        NodeTag::T_FieldStore => {
            let fstore = node as *mut FieldStore;

            if WALK!((*fstore).arg) {
                return true;
            }
            if WALK!((*fstore).newvals) {
                return true;
            }
        }
        NodeTag::T_RelabelType => {
            return WALK!((*(node as *mut RelabelType)).arg);
        }
        NodeTag::T_CoerceViaIO => {
            return WALK!((*(node as *mut CoerceViaIO)).arg);
        }
        NodeTag::T_ArrayCoerceExpr => {
            let acoerce = node as *mut ArrayCoerceExpr;

            if WALK!((*acoerce).arg) {
                return true;
            }
            if WALK!((*acoerce).elemexpr) {
                return true;
            }
        }
        NodeTag::T_ConvertRowtypeExpr => {
            return WALK!((*(node as *mut ConvertRowtypeExpr)).arg);
        }
        NodeTag::T_CollateExpr => {
            return WALK!((*(node as *mut CollateExpr)).arg);
        }
        NodeTag::T_CaseExpr => {
            let caseexpr = node as *mut CaseExpr;

            if WALK!((*caseexpr).arg) {
                return true;
            }
            /* we assume walker doesn't care about CaseWhens, either */
            foreach!(temp, (*caseexpr).args, {
                let when = lfirst_node!(CaseWhen, T_CaseWhen, current_cell!(temp));

                if WALK!((*when).expr) {
                    return true;
                }
                if WALK!((*when).result) {
                    return true;
                }
            });
            if WALK!((*caseexpr).defresult) {
                return true;
            }
        }
        NodeTag::T_ArrayExpr => {
            return WALK!((*(node as *mut ArrayExpr)).elements);
        }
        NodeTag::T_RowExpr => {
            /* Assume colnames isn't interesting */
            return WALK!((*(node as *mut RowExpr)).args);
        }
        NodeTag::T_RowCompareExpr => {
            let rcexpr = node as *mut RowCompareExpr;

            if WALK!((*rcexpr).largs) {
                return true;
            }
            if WALK!((*rcexpr).rargs) {
                return true;
            }
        }
        NodeTag::T_CoalesceExpr => {
            return WALK!((*(node as *mut CoalesceExpr)).args);
        }
        NodeTag::T_MinMaxExpr => {
            return WALK!((*(node as *mut MinMaxExpr)).args);
        }
        NodeTag::T_XmlExpr => {
            let xexpr = node as *mut XmlExpr;

            if WALK!((*xexpr).named_args) {
                return true;
            }
            /* we assume walker doesn't care about arg_names */
            if WALK!((*xexpr).args) {
                return true;
            }
        }
        NodeTag::T_JsonValueExpr => {
            let jve = node as *mut JsonValueExpr;

            if WALK!((*jve).raw_expr) {
                return true;
            }
            if WALK!((*jve).formatted_expr) {
                return true;
            }
        }
        NodeTag::T_JsonConstructorExpr => {
            let ctor = node as *mut JsonConstructorExpr;

            if WALK!((*ctor).args) {
                return true;
            }
            if WALK!((*ctor).func) {
                return true;
            }
            if WALK!((*ctor).coercion) {
                return true;
            }
        }
        NodeTag::T_JsonIsPredicate => {
            return WALK!((*(node as *mut JsonIsPredicate)).expr);
        }
        NodeTag::T_JsonExpr => {
            let jexpr = node as *mut JsonExpr;

            if WALK!((*jexpr).formatted_expr) {
                return true;
            }
            if WALK!((*jexpr).path_spec) {
                return true;
            }
            if WALK!((*jexpr).passing_values) {
                return true;
            }
            /* we assume walker doesn't care about passing_names */
            if WALK!((*jexpr).on_empty) {
                return true;
            }
            if WALK!((*jexpr).on_error) {
                return true;
            }
        }
        NodeTag::T_JsonBehavior => {
            let behavior = node as *mut JsonBehavior;

            if WALK!((*behavior).expr) {
                return true;
            }
        }
        NodeTag::T_NullTest => {
            return WALK!((*(node as *mut NullTest)).arg);
        }
        NodeTag::T_BooleanTest => {
            return WALK!((*(node as *mut BooleanTest)).arg);
        }
        NodeTag::T_CoerceToDomain => {
            return WALK!((*(node as *mut CoerceToDomain)).arg);
        }
        NodeTag::T_TargetEntry => {
            return WALK!((*(node as *mut TargetEntry)).expr);
        }
        NodeTag::T_Query => {
            /* Do nothing with a sub-Query, per discussion above */
        }
        NodeTag::T_WindowClause => {
            let wc = node as *mut WindowClause;

            if WALK!((*wc).partitionClause) {
                return true;
            }
            if WALK!((*wc).orderClause) {
                return true;
            }
            if WALK!((*wc).startOffset) {
                return true;
            }
            if WALK!((*wc).endOffset) {
                return true;
            }
        }
        NodeTag::T_CTECycleClause => {
            let cc = node as *mut CTECycleClause;

            if WALK!((*cc).cycle_mark_value) {
                return true;
            }
            if WALK!((*cc).cycle_mark_default) {
                return true;
            }
        }
        NodeTag::T_CommonTableExpr => {
            let cte = node as *mut CommonTableExpr;

            /*
             * Invoke the walker on the CTE's Query node, so it can
             * recurse into the sub-query if it wants to.
             */
            if WALK!((*cte).ctequery) {
                return true;
            }

            if WALK!((*cte).search_clause) {
                return true;
            }
            if WALK!((*cte).cycle_clause) {
                return true;
            }
        }
        NodeTag::T_JsonKeyValue => {
            let kv = node as *mut JsonKeyValue;

            if WALK!((*kv).key) {
                return true;
            }
            if WALK!((*kv).value) {
                return true;
            }
        }
        NodeTag::T_JsonObjectConstructor => {
            let ctor = node as *mut JsonObjectConstructor;

            if LIST_WALK!((*ctor).exprs) {
                return true;
            }
        }
        NodeTag::T_JsonArrayConstructor => {
            let ctor = node as *mut JsonArrayConstructor;

            if LIST_WALK!((*ctor).exprs) {
                return true;
            }
        }
        NodeTag::T_JsonArrayQueryConstructor => {
            let ctor = node as *mut JsonArrayQueryConstructor;

            if WALK!((*ctor).query) {
                return true;
            }
        }
        NodeTag::T_JsonAggConstructor => {
            let ctor = node as *mut JsonAggConstructor;

            if WALK!((*ctor).agg_filter) {
                return true;
            }
            if WALK!((*ctor).agg_order) {
                return true;
            }
            if WALK!((*ctor).over) {
                return true;
            }
        }
        NodeTag::T_JsonObjectAgg => {
            let ctor = node as *mut JsonObjectAgg;

            if WALK!((*ctor).constructor) {
                return true;
            }
            if WALK!((*ctor).arg) {
                return true;
            }
        }
        NodeTag::T_JsonArrayAgg => {
            let ctor = node as *mut JsonArrayAgg;

            if WALK!((*ctor).constructor) {
                return true;
            }
            if WALK!((*ctor).arg) {
                return true;
            }
        }
        NodeTag::T_PartitionBoundSpec => {
            let pbs = node as *mut PartitionBoundSpec;

            if WALK!((*pbs).listdatums) {
                return true;
            }
            if WALK!((*pbs).lowerdatums) {
                return true;
            }
            if WALK!((*pbs).upperdatums) {
                return true;
            }
        }
        NodeTag::T_PartitionRangeDatum => {
            let prd = node as *mut PartitionRangeDatum;

            if WALK!((*prd).value) {
                return true;
            }
        }
        NodeTag::T_List => {
            foreach!(temp, node as *mut List, {
                if WALK!(lfirst(current_cell!(temp))) {
                    return true;
                }
            });
        }
        NodeTag::T_FromExpr => {
            let from = node as *mut FromExpr;

            if LIST_WALK!((*from).fromlist) {
                return true;
            }
            if WALK!((*from).quals) {
                return true;
            }
        }
        NodeTag::T_OnConflictExpr => {
            let onconflict = node as *mut OnConflictExpr;

            if WALK!((*onconflict).arbiterElems) {
                return true;
            }
            if WALK!((*onconflict).arbiterWhere) {
                return true;
            }
            if WALK!((*onconflict).onConflictSet) {
                return true;
            }
            if WALK!((*onconflict).onConflictWhere) {
                return true;
            }
            if WALK!((*onconflict).exclRelTlist) {
                return true;
            }
        }
        NodeTag::T_MergeAction => {
            let action = node as *mut MergeAction;

            if WALK!((*action).qual) {
                return true;
            }
            if WALK!((*action).targetList) {
                return true;
            }
        }
        NodeTag::T_PartitionPruneStepOp => {
            let opstep = node as *mut PartitionPruneStepOp;

            if WALK!((*opstep).exprs) {
                return true;
            }
        }
        NodeTag::T_PartitionPruneStepCombine => {
            /* no expression subnodes */
        }
        NodeTag::T_JoinExpr => {
            let join = node as *mut JoinExpr;

            if WALK!((*join).larg) {
                return true;
            }
            if WALK!((*join).rarg) {
                return true;
            }
            if WALK!((*join).quals) {
                return true;
            }

            /*
             * alias clause, using list are deemed uninteresting.
             */
        }
        NodeTag::T_SetOperationStmt => {
            let setop = node as *mut SetOperationStmt;

            if WALK!((*setop).larg) {
                return true;
            }
            if WALK!((*setop).rarg) {
                return true;
            }

            /* groupClauses are deemed uninteresting */
        }
        NodeTag::T_IndexClause => {
            let iclause = node as *mut IndexClause;

            if WALK!((*iclause).rinfo) {
                return true;
            }
            if LIST_WALK!((*iclause).indexquals) {
                return true;
            }
        }
        NodeTag::T_PlaceHolderVar => {
            return WALK!((*(node as *mut PlaceHolderVar)).phexpr);
        }
        NodeTag::T_InferenceElem => {
            return WALK!((*(node as *mut InferenceElem)).expr);
        }
        NodeTag::T_ReturningExpr => {
            return WALK!((*(node as *mut ReturningExpr)).retexpr);
        }
        NodeTag::T_AppendRelInfo => {
            let appinfo = node as *mut AppendRelInfo;

            if LIST_WALK!((*appinfo).translated_vars) {
                return true;
            }
        }
        NodeTag::T_PlaceHolderInfo => {
            return WALK!((*(node as *mut PlaceHolderInfo)).ph_var);
        }
        NodeTag::T_RangeTblFunction => {
            return WALK!((*(node as *mut RangeTblFunction)).funcexpr);
        }
        NodeTag::T_TableSampleClause => {
            let tsc = node as *mut TableSampleClause;

            if LIST_WALK!((*tsc).args) {
                return true;
            }
            if WALK!((*tsc).repeatable) {
                return true;
            }
        }
        NodeTag::T_TableFunc => {
            let tf = node as *mut TableFunc;

            if WALK!((*tf).ns_uris) {
                return true;
            }
            if WALK!((*tf).docexpr) {
                return true;
            }
            if WALK!((*tf).rowexpr) {
                return true;
            }
            if WALK!((*tf).colexprs) {
                return true;
            }
            if WALK!((*tf).coldefexprs) {
                return true;
            }
            if WALK!((*tf).colvalexprs) {
                return true;
            }
            if WALK!((*tf).passingvalexprs) {
                return true;
            }
        }
        _ => {
            elog!(ERROR, "unrecognized node type: {}", nodeTag(node) as c_int);
        }
    }
    false
}

/// `expression_tree_walker(n, w, c)` (nodeFuncs.h macro).  In C this macro casts
/// the strongly-typed walker to `tree_walker_callback`; here the callback is
/// already that type, so this is a thin pass-through to the `_impl`.
///
/// # Safety
/// `node` must be NULL or a valid node tree; `walker` must be a valid callback.
#[inline]
pub unsafe fn expression_tree_walker(
    node: *mut Node,
    walker: tree_walker_callback,
    context: *mut c_void,
) -> bool {
    expression_tree_walker_impl(node, walker, context)
}

/*
 * query_tree_walker --- initiate a walk of a Query's expressions
 *
 * This routine exists just to reduce the number of places that need to know
 * where all the expression subtrees of a Query are.  Note it can be used
 * for starting a walk at top level of a Query regardless of whether the
 * walker intends to descend into subqueries.  It is also useful for
 * descending into subqueries within a walker.
 *
 * Some callers want to suppress visitation of certain items in the sub-Query,
 * typically because they need to process them specially, or don't actually
 * want to recurse into subqueries.  This is supported by the flags argument,
 * which is the bitwise OR of flag values to add or suppress visitation of
 * indicated items.  (More flag bits may be added as needed.)
 */
pub unsafe fn query_tree_walker_impl(
    query: *mut Query,
    walker: tree_walker_callback,
    context: *mut c_void,
    flags: c_int,
) -> bool {
    let w = walker.unwrap();
    macro_rules! WALK {
        ($n:expr) => {
            w($n as *mut Node, context)
        };
    }

    Assert!(!query.is_null() && IsA!(query, T_Query));

    /*
     * We don't walk any utilityStmt here. However, we can't easily assert
     * that it is absent, since there are at least two code paths by which
     * action statements from CREATE RULE end up here, and NOTIFY is allowed
     * in a rule action.
     */

    if WALK!((*query).targetList) {
        return true;
    }
    if WALK!((*query).withCheckOptions) {
        return true;
    }
    if WALK!((*query).onConflict) {
        return true;
    }
    if WALK!((*query).mergeActionList) {
        return true;
    }
    if WALK!((*query).mergeJoinCondition) {
        return true;
    }
    if WALK!((*query).returningList) {
        return true;
    }
    if WALK!((*query).jointree) {
        return true;
    }
    if WALK!((*query).setOperations) {
        return true;
    }
    if WALK!((*query).havingQual) {
        return true;
    }
    if WALK!((*query).limitOffset) {
        return true;
    }
    if WALK!((*query).limitCount) {
        return true;
    }

    /*
     * Most callers aren't interested in SortGroupClause nodes since those
     * don't contain actual expressions. However they do contain OIDs which
     * may be needed by dependency walkers etc.
     */
    if (flags & QTW_EXAMINE_SORTGROUP) != 0 {
        if WALK!((*query).groupClause) {
            return true;
        }
        if WALK!((*query).windowClause) {
            return true;
        }
        if WALK!((*query).sortClause) {
            return true;
        }
        if WALK!((*query).distinctClause) {
            return true;
        }
    } else {
        /*
         * But we need to walk the expressions under WindowClause nodes even
         * if we're not interested in SortGroupClause nodes.
         */
        foreach!(lc, (*query).windowClause, {
            let wc = lfirst_node!(WindowClause, T_WindowClause, current_cell!(lc));

            if WALK!((*wc).startOffset) {
                return true;
            }
            if WALK!((*wc).endOffset) {
                return true;
            }
        });
    }

    /*
     * groupingSets and rowMarks are not walked:
     *
     * groupingSets contain only ressortgrouprefs (integers) which are
     * meaningless without the corresponding groupClause or tlist.
     * Accordingly, any walker that needs to care about them needs to handle
     * them itself in its Query processing.
     *
     * rowMarks is not walked because it contains only rangetable indexes (and
     * flags etc.) and therefore should be handled at Query level similarly.
     */

    if (flags & QTW_IGNORE_CTE_SUBQUERIES) == 0 {
        if WALK!((*query).cteList) {
            return true;
        }
    }
    if (flags & QTW_IGNORE_RANGE_TABLE) == 0 {
        if range_table_walker((*query).rtable, walker, context, flags) {
            return true;
        }
    }
    false
}

/// `query_tree_walker(q, w, c, f)` (nodeFuncs.h macro): thin wrapper.
///
/// # Safety
/// `query` must be a valid Query; `walker` a valid callback.
#[inline]
pub unsafe fn query_tree_walker(
    query: *mut Query,
    walker: tree_walker_callback,
    context: *mut c_void,
    flags: c_int,
) -> bool {
    query_tree_walker_impl(query, walker, context, flags)
}

/*
 * range_table_walker is just the part of query_tree_walker that scans
 * a query's rangetable.  This is split out since it can be useful on
 * its own.
 */
pub unsafe fn range_table_walker_impl(
    rtable: *mut List,
    walker: tree_walker_callback,
    context: *mut c_void,
    flags: c_int,
) -> bool {
    foreach!(rt, rtable, {
        let rte = lfirst_node!(RangeTblEntry, T_RangeTblEntry, current_cell!(rt));

        if range_table_entry_walker(rte, walker, context, flags) {
            return true;
        }
    });
    false
}

/// `range_table_walker(rt, w, c, f)` (nodeFuncs.h macro): thin wrapper.
///
/// # Safety
/// `rtable` must be NIL or a valid List of RangeTblEntry; `walker` valid.
#[inline]
pub unsafe fn range_table_walker(
    rtable: *mut List,
    walker: tree_walker_callback,
    context: *mut c_void,
    flags: c_int,
) -> bool {
    range_table_walker_impl(rtable, walker, context, flags)
}

/*
 * Some callers even want to scan the expressions in individual RTEs.
 */
pub unsafe fn range_table_entry_walker_impl(
    rte: *mut RangeTblEntry,
    walker: tree_walker_callback,
    context: *mut c_void,
    flags: c_int,
) -> bool {
    let w = walker.unwrap();
    macro_rules! WALK {
        ($n:expr) => {
            w($n as *mut Node, context)
        };
    }

    /*
     * Walkers might need to examine the RTE node itself either before or
     * after visiting its contents (or, conceivably, both).  Note that if you
     * specify neither flag, the walker won't be called on the RTE at all.
     */
    if (flags & QTW_EXAMINE_RTES_BEFORE) != 0 {
        if WALK!(rte) {
            return true;
        }
    }

    match (*rte).rtekind {
        RTEKind::RTE_RELATION => {
            if WALK!((*rte).tablesample) {
                return true;
            }
        }
        RTEKind::RTE_SUBQUERY => {
            if (flags & QTW_IGNORE_RT_SUBQUERIES) == 0 {
                if WALK!((*rte).subquery) {
                    return true;
                }
            }
        }
        RTEKind::RTE_JOIN => {
            if (flags & QTW_IGNORE_JOINALIASES) == 0 {
                if WALK!((*rte).joinaliasvars) {
                    return true;
                }
            }
        }
        RTEKind::RTE_FUNCTION => {
            if WALK!((*rte).functions) {
                return true;
            }
        }
        RTEKind::RTE_TABLEFUNC => {
            if WALK!((*rte).tablefunc) {
                return true;
            }
        }
        RTEKind::RTE_VALUES => {
            if WALK!((*rte).values_lists) {
                return true;
            }
        }
        RTEKind::RTE_CTE | RTEKind::RTE_NAMEDTUPLESTORE | RTEKind::RTE_RESULT => {
            /* nothing to do */
        }
        RTEKind::RTE_GROUP => {
            if (flags & QTW_IGNORE_GROUPEXPRS) == 0 {
                if WALK!((*rte).groupexprs) {
                    return true;
                }
            }
        }
    }

    if WALK!((*rte).securityQuals) {
        return true;
    }

    if (flags & QTW_EXAMINE_RTES_AFTER) != 0 {
        if WALK!(rte) {
            return true;
        }
    }

    false
}

/// `range_table_entry_walker(r, w, c, f)` (nodeFuncs.h macro): thin wrapper.
///
/// # Safety
/// `rte` must be a valid RangeTblEntry; `walker` valid.
#[inline]
pub unsafe fn range_table_entry_walker(
    rte: *mut RangeTblEntry,
    walker: tree_walker_callback,
    context: *mut c_void,
    flags: c_int,
) -> bool {
    range_table_entry_walker_impl(rte, walker, context, flags)
}

/*
 * expression_tree_mutator() is designed to support routines that make a
 * modified copy of an expression tree, with some nodes being added,
 * removed, or replaced by new subtrees.  The original tree is (normally)
 * not changed.  Each recursion level is responsible for returning a copy of
 * (or appropriately modified substitute for) the subtree it is handed.
 * A mutator routine should look like this:
 *
 * Node * my_mutator (Node *node, my_struct *context)
 * {
 *		if (node == NULL)
 *			return NULL;
 *		// check for nodes that special work is required for, eg:
 *		if (IsA(node, Var))
 *		{
 *			... create and return modified copy of Var node
 *		}
 *		else if (IsA(node, ...))
 *		{
 *			... do special transformations of other node types
 *		}
 *		// for any node type not specially processed, do:
 *		return expression_tree_mutator(node, my_mutator, context);
 * }
 *
 * The "context" argument points to a struct that holds whatever context
 * information the mutator routine needs --- it can be used to return extra
 * data gathered by the mutator, too.  This argument is not touched by
 * expression_tree_mutator, but it is passed down to recursive sub-invocations
 * of my_mutator.  The tree walk is started from a setup routine that
 * fills in the appropriate context struct, calls my_mutator with the
 * top-level node of the tree, and does any required post-processing.
 *
 * Each level of recursion must return an appropriately modified Node.
 * If expression_tree_mutator() is called, it will make an exact copy
 * of the given Node, but invoke my_mutator() to copy the sub-node(s)
 * of that Node.  In this way, my_mutator() has full control over the
 * copying process but need not directly deal with expression trees
 * that it has no interest in.
 *
 * Just as for expression_tree_walker, the node types handled by
 * expression_tree_mutator include all those normally found in target lists
 * and qualifier clauses during the planning stage.
 *
 * expression_tree_mutator will handle SubLink nodes by recursing normally
 * into the "testexpr" subtree (which is an expression belonging to the outer
 * plan).  It will also call the mutator on the sub-Query node; however, when
 * expression_tree_mutator itself is called on a Query node, it does nothing
 * and returns the unmodified Query node.  The net effect is that unless the
 * mutator does something special at a Query node, sub-selects will not be
 * visited or modified; the original sub-select will be linked to by the new
 * SubLink node.  Mutators that want to descend into sub-selects will usually
 * do so by recognizing Query nodes and calling query_tree_mutator (below).
 *
 * expression_tree_mutator will handle a SubPlan node by recursing into the
 * "testexpr" and the "args" list (which belong to the outer plan), but it
 * will simply copy the link to the inner plan, since that's typically what
 * expression tree mutators want.  A mutator that wants to modify the subplan
 * can force appropriate behavior by recognizing SubPlan expression nodes
 * and doing the right thing.
 */
pub unsafe fn expression_tree_mutator_impl(
    node: *mut Node,
    mutator: tree_mutator_callback,
    context: *mut c_void,
) -> *mut Node {
    /*
     * The mutator has already decided not to modify the current node, but we
     * must call the mutator for any sub-nodes.
     */
    let m = mutator.unwrap();

    /*
     * FLATCOPY(newnode, node, nodetype): palloc a fresh nodetype and shallow-
     * copy the bytes from `node`.  Binds `$newnode` to a `*mut $nodetype`.
     */
    macro_rules! FLATCOPY {
        ($newnode:ident, $node:expr, $nodetype:ty) => {
            let $newnode: *mut $nodetype =
                palloc(core::mem::size_of::<$nodetype>()) as *mut $nodetype;
            core::ptr::copy_nonoverlapping(
                $node as *const $nodetype,
                $newnode,
                1,
            );
        };
    }
    /*
     * MUTATE(newfield, oldfield, fieldtype):
     *     newfield = (fieldtype) mutator((Node *) oldfield, context)
     */
    macro_rules! MUTATE {
        ($newfield:expr, $oldfield:expr, $fieldtype:ty) => {
            $newfield = m($oldfield as *mut Node, context) as $fieldtype
        };
    }

    if node.is_null() {
        return null_mut();
    }

    /* Guard against stack overflow due to overly complex expressions */
    check_stack_depth();

    match nodeTag(node) {
        /*
         * Primitive node types with no expression subnodes.  Var and
         * Const are frequent enough to deserve special cases, the others
         * we just use copyObject for.
         */
        NodeTag::T_Var => {
            let var = node as *mut Var;
            FLATCOPY!(newnode, var, Var);
            /* Assume we need not copy the varnullingrels bitmapset */
            return newnode as *mut Node;
        }
        NodeTag::T_Const => {
            let oldnode = node as *mut Const;
            FLATCOPY!(newnode, oldnode, Const);
            /* XXX we don't bother with datumCopy; should we? */
            return newnode as *mut Node;
        }
        NodeTag::T_Param
        | NodeTag::T_CaseTestExpr
        | NodeTag::T_SQLValueFunction
        | NodeTag::T_JsonFormat
        | NodeTag::T_CoerceToDomainValue
        | NodeTag::T_SetToDefault
        | NodeTag::T_CurrentOfExpr
        | NodeTag::T_NextValueExpr
        | NodeTag::T_RangeTblRef
        | NodeTag::T_SortGroupClause
        | NodeTag::T_CTESearchClause
        | NodeTag::T_MergeSupportFunc => {
            return copyObject(node);
        }
        NodeTag::T_WithCheckOption => {
            let wco = node as *mut WithCheckOption;
            FLATCOPY!(newnode, wco, WithCheckOption);
            MUTATE!((*newnode).qual, (*wco).qual, *mut Node);
            return newnode as *mut Node;
        }
        NodeTag::T_Aggref => {
            let aggref = node as *mut Aggref;
            FLATCOPY!(newnode, aggref, Aggref);
            /* assume mutation doesn't change types of arguments */
            (*newnode).aggargtypes = list_copy((*aggref).aggargtypes);
            MUTATE!((*newnode).aggdirectargs, (*aggref).aggdirectargs, *mut List);
            MUTATE!((*newnode).args, (*aggref).args, *mut List);
            MUTATE!((*newnode).aggorder, (*aggref).aggorder, *mut List);
            MUTATE!((*newnode).aggdistinct, (*aggref).aggdistinct, *mut List);
            MUTATE!((*newnode).aggfilter, (*aggref).aggfilter, *mut Expr);
            return newnode as *mut Node;
        }
        NodeTag::T_GroupingFunc => {
            let grouping = node as *mut GroupingFunc;
            FLATCOPY!(newnode, grouping, GroupingFunc);
            MUTATE!((*newnode).args, (*grouping).args, *mut List);

            /*
             * We assume here that mutating the arguments does not change
             * the semantics, i.e. that the arguments are not mutated in a
             * way that makes them semantically different from their
             * previously matching expressions in the GROUP BY clause.
             *
             * If a mutator somehow wanted to do this, it would have to
             * handle the refs and cols lists itself as appropriate.
             */
            (*newnode).refs = list_copy((*grouping).refs);
            (*newnode).cols = list_copy((*grouping).cols);

            return newnode as *mut Node;
        }
        NodeTag::T_WindowFunc => {
            let wfunc = node as *mut WindowFunc;
            FLATCOPY!(newnode, wfunc, WindowFunc);
            MUTATE!((*newnode).args, (*wfunc).args, *mut List);
            MUTATE!((*newnode).aggfilter, (*wfunc).aggfilter, *mut Expr);
            return newnode as *mut Node;
        }
        NodeTag::T_WindowFuncRunCondition => {
            let wfuncrc = node as *mut WindowFuncRunCondition;
            FLATCOPY!(newnode, wfuncrc, WindowFuncRunCondition);
            MUTATE!((*newnode).arg, (*wfuncrc).arg, *mut Expr);
            return newnode as *mut Node;
        }
        NodeTag::T_SubscriptingRef => {
            let sbsref = node as *mut SubscriptingRef;
            FLATCOPY!(newnode, sbsref, SubscriptingRef);
            MUTATE!(
                (*newnode).refupperindexpr,
                (*sbsref).refupperindexpr,
                *mut List
            );
            MUTATE!(
                (*newnode).reflowerindexpr,
                (*sbsref).reflowerindexpr,
                *mut List
            );
            MUTATE!((*newnode).refexpr, (*sbsref).refexpr, *mut Expr);
            MUTATE!((*newnode).refassgnexpr, (*sbsref).refassgnexpr, *mut Expr);

            return newnode as *mut Node;
        }
        NodeTag::T_FuncExpr => {
            let expr = node as *mut FuncExpr;
            FLATCOPY!(newnode, expr, FuncExpr);
            MUTATE!((*newnode).args, (*expr).args, *mut List);
            return newnode as *mut Node;
        }
        NodeTag::T_NamedArgExpr => {
            let nexpr = node as *mut NamedArgExpr;
            FLATCOPY!(newnode, nexpr, NamedArgExpr);
            MUTATE!((*newnode).arg, (*nexpr).arg, *mut Expr);
            return newnode as *mut Node;
        }
        NodeTag::T_OpExpr => {
            let expr = node as *mut OpExpr;
            FLATCOPY!(newnode, expr, OpExpr);
            MUTATE!((*newnode).args, (*expr).args, *mut List);
            return newnode as *mut Node;
        }
        NodeTag::T_DistinctExpr => {
            let expr = node as *mut DistinctExpr;
            FLATCOPY!(newnode, expr, DistinctExpr);
            MUTATE!((*newnode).args, (*expr).args, *mut List);
            return newnode as *mut Node;
        }
        NodeTag::T_NullIfExpr => {
            let expr = node as *mut NullIfExpr;
            FLATCOPY!(newnode, expr, NullIfExpr);
            MUTATE!((*newnode).args, (*expr).args, *mut List);
            return newnode as *mut Node;
        }
        NodeTag::T_ScalarArrayOpExpr => {
            let expr = node as *mut ScalarArrayOpExpr;
            FLATCOPY!(newnode, expr, ScalarArrayOpExpr);
            MUTATE!((*newnode).args, (*expr).args, *mut List);
            return newnode as *mut Node;
        }
        NodeTag::T_BoolExpr => {
            let expr = node as *mut BoolExpr;
            FLATCOPY!(newnode, expr, BoolExpr);
            MUTATE!((*newnode).args, (*expr).args, *mut List);
            return newnode as *mut Node;
        }
        NodeTag::T_SubLink => {
            let sublink = node as *mut SubLink;
            FLATCOPY!(newnode, sublink, SubLink);
            MUTATE!((*newnode).testexpr, (*sublink).testexpr, *mut Node);

            /*
             * Also invoke the mutator on the sublink's Query node, so it
             * can recurse into the sub-query if it wants to.
             */
            MUTATE!((*newnode).subselect, (*sublink).subselect, *mut Node);
            return newnode as *mut Node;
        }
        NodeTag::T_SubPlan => {
            let subplan = node as *mut SubPlan;
            FLATCOPY!(newnode, subplan, SubPlan);
            /* transform testexpr */
            MUTATE!((*newnode).testexpr, (*subplan).testexpr, *mut Node);
            /* transform args list (params to be passed to subplan) */
            MUTATE!((*newnode).args, (*subplan).args, *mut List);
            /* but not the sub-Plan itself, which is referenced as-is */
            return newnode as *mut Node;
        }
        NodeTag::T_AlternativeSubPlan => {
            let asplan = node as *mut AlternativeSubPlan;
            FLATCOPY!(newnode, asplan, AlternativeSubPlan);
            MUTATE!((*newnode).subplans, (*asplan).subplans, *mut List);
            return newnode as *mut Node;
        }
        NodeTag::T_FieldSelect => {
            let fselect = node as *mut FieldSelect;
            FLATCOPY!(newnode, fselect, FieldSelect);
            MUTATE!((*newnode).arg, (*fselect).arg, *mut Expr);
            return newnode as *mut Node;
        }
        NodeTag::T_FieldStore => {
            let fstore = node as *mut FieldStore;
            FLATCOPY!(newnode, fstore, FieldStore);
            MUTATE!((*newnode).arg, (*fstore).arg, *mut Expr);
            MUTATE!((*newnode).newvals, (*fstore).newvals, *mut List);
            (*newnode).fieldnums = list_copy((*fstore).fieldnums);
            return newnode as *mut Node;
        }
        NodeTag::T_RelabelType => {
            let relabel = node as *mut RelabelType;
            FLATCOPY!(newnode, relabel, RelabelType);
            MUTATE!((*newnode).arg, (*relabel).arg, *mut Expr);
            return newnode as *mut Node;
        }
        NodeTag::T_CoerceViaIO => {
            let iocoerce = node as *mut CoerceViaIO;
            FLATCOPY!(newnode, iocoerce, CoerceViaIO);
            MUTATE!((*newnode).arg, (*iocoerce).arg, *mut Expr);
            return newnode as *mut Node;
        }
        NodeTag::T_ArrayCoerceExpr => {
            let acoerce = node as *mut ArrayCoerceExpr;
            FLATCOPY!(newnode, acoerce, ArrayCoerceExpr);
            MUTATE!((*newnode).arg, (*acoerce).arg, *mut Expr);
            MUTATE!((*newnode).elemexpr, (*acoerce).elemexpr, *mut Expr);
            return newnode as *mut Node;
        }
        NodeTag::T_ConvertRowtypeExpr => {
            let convexpr = node as *mut ConvertRowtypeExpr;
            FLATCOPY!(newnode, convexpr, ConvertRowtypeExpr);
            MUTATE!((*newnode).arg, (*convexpr).arg, *mut Expr);
            return newnode as *mut Node;
        }
        NodeTag::T_CollateExpr => {
            let collate = node as *mut CollateExpr;
            FLATCOPY!(newnode, collate, CollateExpr);
            MUTATE!((*newnode).arg, (*collate).arg, *mut Expr);
            return newnode as *mut Node;
        }
        NodeTag::T_CaseExpr => {
            let caseexpr = node as *mut CaseExpr;
            FLATCOPY!(newnode, caseexpr, CaseExpr);
            MUTATE!((*newnode).arg, (*caseexpr).arg, *mut Expr);
            MUTATE!((*newnode).args, (*caseexpr).args, *mut List);
            MUTATE!((*newnode).defresult, (*caseexpr).defresult, *mut Expr);
            return newnode as *mut Node;
        }
        NodeTag::T_CaseWhen => {
            let casewhen = node as *mut CaseWhen;
            FLATCOPY!(newnode, casewhen, CaseWhen);
            MUTATE!((*newnode).expr, (*casewhen).expr, *mut Expr);
            MUTATE!((*newnode).result, (*casewhen).result, *mut Expr);
            return newnode as *mut Node;
        }
        NodeTag::T_ArrayExpr => {
            let arrayexpr = node as *mut ArrayExpr;
            FLATCOPY!(newnode, arrayexpr, ArrayExpr);
            MUTATE!((*newnode).elements, (*arrayexpr).elements, *mut List);
            return newnode as *mut Node;
        }
        NodeTag::T_RowExpr => {
            let rowexpr = node as *mut RowExpr;
            FLATCOPY!(newnode, rowexpr, RowExpr);
            MUTATE!((*newnode).args, (*rowexpr).args, *mut List);
            /* Assume colnames needn't be duplicated */
            return newnode as *mut Node;
        }
        NodeTag::T_RowCompareExpr => {
            let rcexpr = node as *mut RowCompareExpr;
            FLATCOPY!(newnode, rcexpr, RowCompareExpr);
            MUTATE!((*newnode).largs, (*rcexpr).largs, *mut List);
            MUTATE!((*newnode).rargs, (*rcexpr).rargs, *mut List);
            return newnode as *mut Node;
        }
        NodeTag::T_CoalesceExpr => {
            let coalesceexpr = node as *mut CoalesceExpr;
            FLATCOPY!(newnode, coalesceexpr, CoalesceExpr);
            MUTATE!((*newnode).args, (*coalesceexpr).args, *mut List);
            return newnode as *mut Node;
        }
        NodeTag::T_MinMaxExpr => {
            let minmaxexpr = node as *mut MinMaxExpr;
            FLATCOPY!(newnode, minmaxexpr, MinMaxExpr);
            MUTATE!((*newnode).args, (*minmaxexpr).args, *mut List);
            return newnode as *mut Node;
        }
        NodeTag::T_XmlExpr => {
            let xexpr = node as *mut XmlExpr;
            FLATCOPY!(newnode, xexpr, XmlExpr);
            MUTATE!((*newnode).named_args, (*xexpr).named_args, *mut List);
            /* assume mutator does not care about arg_names */
            MUTATE!((*newnode).args, (*xexpr).args, *mut List);
            return newnode as *mut Node;
        }
        NodeTag::T_JsonReturning => {
            let jr = node as *mut JsonReturning;
            FLATCOPY!(newnode, jr, JsonReturning);
            MUTATE!((*newnode).format, (*jr).format, *mut JsonFormat);

            return newnode as *mut Node;
        }
        NodeTag::T_JsonValueExpr => {
            let jve = node as *mut JsonValueExpr;
            FLATCOPY!(newnode, jve, JsonValueExpr);
            MUTATE!((*newnode).raw_expr, (*jve).raw_expr, *mut Expr);
            MUTATE!((*newnode).formatted_expr, (*jve).formatted_expr, *mut Expr);
            MUTATE!((*newnode).format, (*jve).format, *mut JsonFormat);

            return newnode as *mut Node;
        }
        NodeTag::T_JsonConstructorExpr => {
            let jce = node as *mut JsonConstructorExpr;
            FLATCOPY!(newnode, jce, JsonConstructorExpr);
            MUTATE!((*newnode).args, (*jce).args, *mut List);
            MUTATE!((*newnode).func, (*jce).func, *mut Expr);
            MUTATE!((*newnode).coercion, (*jce).coercion, *mut Expr);
            MUTATE!((*newnode).returning, (*jce).returning, *mut JsonReturning);

            return newnode as *mut Node;
        }
        NodeTag::T_JsonIsPredicate => {
            let pred = node as *mut JsonIsPredicate;
            FLATCOPY!(newnode, pred, JsonIsPredicate);
            MUTATE!((*newnode).expr, (*pred).expr, *mut Node);
            MUTATE!((*newnode).format, (*pred).format, *mut JsonFormat);

            return newnode as *mut Node;
        }
        NodeTag::T_JsonExpr => {
            let jexpr = node as *mut JsonExpr;
            FLATCOPY!(newnode, jexpr, JsonExpr);
            MUTATE!((*newnode).formatted_expr, (*jexpr).formatted_expr, *mut Node);
            MUTATE!((*newnode).path_spec, (*jexpr).path_spec, *mut Node);
            MUTATE!((*newnode).passing_values, (*jexpr).passing_values, *mut List);
            /* assume mutator does not care about passing_names */
            MUTATE!((*newnode).on_empty, (*jexpr).on_empty, *mut JsonBehavior);
            MUTATE!((*newnode).on_error, (*jexpr).on_error, *mut JsonBehavior);
            return newnode as *mut Node;
        }
        NodeTag::T_JsonBehavior => {
            let behavior = node as *mut JsonBehavior;
            FLATCOPY!(newnode, behavior, JsonBehavior);
            MUTATE!((*newnode).expr, (*behavior).expr, *mut Node);
            return newnode as *mut Node;
        }
        NodeTag::T_NullTest => {
            let ntest = node as *mut NullTest;
            FLATCOPY!(newnode, ntest, NullTest);
            MUTATE!((*newnode).arg, (*ntest).arg, *mut Expr);
            return newnode as *mut Node;
        }
        NodeTag::T_BooleanTest => {
            let btest = node as *mut BooleanTest;
            FLATCOPY!(newnode, btest, BooleanTest);
            MUTATE!((*newnode).arg, (*btest).arg, *mut Expr);
            return newnode as *mut Node;
        }
        NodeTag::T_CoerceToDomain => {
            let ctest = node as *mut CoerceToDomain;
            FLATCOPY!(newnode, ctest, CoerceToDomain);
            MUTATE!((*newnode).arg, (*ctest).arg, *mut Expr);
            return newnode as *mut Node;
        }
        NodeTag::T_ReturningExpr => {
            let rexpr = node as *mut ReturningExpr;
            FLATCOPY!(newnode, rexpr, ReturningExpr);
            MUTATE!((*newnode).retexpr, (*rexpr).retexpr, *mut Expr);
            return newnode as *mut Node;
        }
        NodeTag::T_TargetEntry => {
            let targetentry = node as *mut TargetEntry;
            FLATCOPY!(newnode, targetentry, TargetEntry);
            MUTATE!((*newnode).expr, (*targetentry).expr, *mut Expr);
            return newnode as *mut Node;
        }
        NodeTag::T_Query => {
            /* Do nothing with a sub-Query, per discussion above */
            return node;
        }
        NodeTag::T_WindowClause => {
            let wc = node as *mut WindowClause;
            FLATCOPY!(newnode, wc, WindowClause);
            MUTATE!((*newnode).partitionClause, (*wc).partitionClause, *mut List);
            MUTATE!((*newnode).orderClause, (*wc).orderClause, *mut List);
            MUTATE!((*newnode).startOffset, (*wc).startOffset, *mut Node);
            MUTATE!((*newnode).endOffset, (*wc).endOffset, *mut Node);
            return newnode as *mut Node;
        }
        NodeTag::T_CTECycleClause => {
            let cc = node as *mut CTECycleClause;
            FLATCOPY!(newnode, cc, CTECycleClause);
            MUTATE!((*newnode).cycle_mark_value, (*cc).cycle_mark_value, *mut Node);
            MUTATE!(
                (*newnode).cycle_mark_default,
                (*cc).cycle_mark_default,
                *mut Node
            );
            return newnode as *mut Node;
        }
        NodeTag::T_CommonTableExpr => {
            let cte = node as *mut CommonTableExpr;
            FLATCOPY!(newnode, cte, CommonTableExpr);

            /*
             * Also invoke the mutator on the CTE's Query node, so it can
             * recurse into the sub-query if it wants to.
             */
            MUTATE!((*newnode).ctequery, (*cte).ctequery, *mut Node);

            MUTATE!(
                (*newnode).search_clause,
                (*cte).search_clause,
                *mut CTESearchClause
            );
            MUTATE!(
                (*newnode).cycle_clause,
                (*cte).cycle_clause,
                *mut CTECycleClause
            );

            return newnode as *mut Node;
        }
        NodeTag::T_PartitionBoundSpec => {
            let pbs = node as *mut PartitionBoundSpec;
            FLATCOPY!(newnode, pbs, PartitionBoundSpec);
            MUTATE!((*newnode).listdatums, (*pbs).listdatums, *mut List);
            MUTATE!((*newnode).lowerdatums, (*pbs).lowerdatums, *mut List);
            MUTATE!((*newnode).upperdatums, (*pbs).upperdatums, *mut List);
            return newnode as *mut Node;
        }
        NodeTag::T_PartitionRangeDatum => {
            let prd = node as *mut PartitionRangeDatum;
            FLATCOPY!(newnode, prd, PartitionRangeDatum);
            MUTATE!((*newnode).value, (*prd).value, *mut Node);
            return newnode as *mut Node;
        }
        NodeTag::T_List => {
            /*
             * We assume the mutator isn't interested in the list nodes
             * per se, so just invoke it on each list element. NOTE: this
             * would fail badly on a list with integer elements!
             */
            let mut resultlist: *mut List;

            resultlist = NIL;
            foreach!(temp, node as *mut List, {
                resultlist = lappend(
                    resultlist,
                    m(lfirst(current_cell!(temp)) as *mut Node, context) as *mut c_void,
                );
            });
            return resultlist as *mut Node;
        }
        NodeTag::T_FromExpr => {
            let from = node as *mut FromExpr;
            FLATCOPY!(newnode, from, FromExpr);
            MUTATE!((*newnode).fromlist, (*from).fromlist, *mut List);
            MUTATE!((*newnode).quals, (*from).quals, *mut Node);
            return newnode as *mut Node;
        }
        NodeTag::T_OnConflictExpr => {
            let oc = node as *mut OnConflictExpr;
            FLATCOPY!(newnode, oc, OnConflictExpr);
            MUTATE!((*newnode).arbiterElems, (*oc).arbiterElems, *mut List);
            MUTATE!((*newnode).arbiterWhere, (*oc).arbiterWhere, *mut Node);
            MUTATE!((*newnode).onConflictSet, (*oc).onConflictSet, *mut List);
            MUTATE!((*newnode).onConflictWhere, (*oc).onConflictWhere, *mut Node);
            MUTATE!((*newnode).exclRelTlist, (*oc).exclRelTlist, *mut List);

            return newnode as *mut Node;
        }
        NodeTag::T_MergeAction => {
            let action = node as *mut MergeAction;
            FLATCOPY!(newnode, action, MergeAction);
            MUTATE!((*newnode).qual, (*action).qual, *mut Node);
            MUTATE!((*newnode).targetList, (*action).targetList, *mut List);

            return newnode as *mut Node;
        }
        NodeTag::T_PartitionPruneStepOp => {
            let opstep = node as *mut PartitionPruneStepOp;
            FLATCOPY!(newnode, opstep, PartitionPruneStepOp);
            MUTATE!((*newnode).exprs, (*opstep).exprs, *mut List);

            return newnode as *mut Node;
        }
        NodeTag::T_PartitionPruneStepCombine => {
            /* no expression sub-nodes */
            return copyObject(node);
        }
        NodeTag::T_JoinExpr => {
            let join = node as *mut JoinExpr;
            FLATCOPY!(newnode, join, JoinExpr);
            MUTATE!((*newnode).larg, (*join).larg, *mut Node);
            MUTATE!((*newnode).rarg, (*join).rarg, *mut Node);
            MUTATE!((*newnode).quals, (*join).quals, *mut Node);
            /* We do not mutate alias or using by default */
            return newnode as *mut Node;
        }
        NodeTag::T_SetOperationStmt => {
            let setop = node as *mut SetOperationStmt;
            FLATCOPY!(newnode, setop, SetOperationStmt);
            MUTATE!((*newnode).larg, (*setop).larg, *mut Node);
            MUTATE!((*newnode).rarg, (*setop).rarg, *mut Node);
            /* We do not mutate groupClauses by default */
            return newnode as *mut Node;
        }
        NodeTag::T_IndexClause => {
            let iclause = node as *mut IndexClause;
            FLATCOPY!(newnode, iclause, IndexClause);
            MUTATE!((*newnode).rinfo, (*iclause).rinfo, *mut RestrictInfo);
            MUTATE!((*newnode).indexquals, (*iclause).indexquals, *mut List);
            return newnode as *mut Node;
        }
        NodeTag::T_PlaceHolderVar => {
            let phv = node as *mut PlaceHolderVar;
            FLATCOPY!(newnode, phv, PlaceHolderVar);
            MUTATE!((*newnode).phexpr, (*phv).phexpr, *mut Expr);
            /* Assume we need not copy the relids bitmapsets */
            return newnode as *mut Node;
        }
        NodeTag::T_InferenceElem => {
            let inferenceelemdexpr = node as *mut InferenceElem;
            FLATCOPY!(newnode, inferenceelemdexpr, InferenceElem);
            MUTATE!((*newnode).expr, (*newnode).expr, *mut Node);
            return newnode as *mut Node;
        }
        NodeTag::T_AppendRelInfo => {
            let appinfo = node as *mut AppendRelInfo;
            FLATCOPY!(newnode, appinfo, AppendRelInfo);
            MUTATE!((*newnode).translated_vars, (*appinfo).translated_vars, *mut List);
            /* Assume nothing need be done with parent_colnos[] */
            return newnode as *mut Node;
        }
        NodeTag::T_PlaceHolderInfo => {
            let phinfo = node as *mut PlaceHolderInfo;
            FLATCOPY!(newnode, phinfo, PlaceHolderInfo);
            MUTATE!((*newnode).ph_var, (*phinfo).ph_var, *mut PlaceHolderVar);
            /* Assume we need not copy the relids bitmapsets */
            return newnode as *mut Node;
        }
        NodeTag::T_RangeTblFunction => {
            let rtfunc = node as *mut RangeTblFunction;
            FLATCOPY!(newnode, rtfunc, RangeTblFunction);
            MUTATE!((*newnode).funcexpr, (*rtfunc).funcexpr, *mut Node);
            /* Assume we need not copy the coldef info lists */
            return newnode as *mut Node;
        }
        NodeTag::T_TableSampleClause => {
            let tsc = node as *mut TableSampleClause;
            FLATCOPY!(newnode, tsc, TableSampleClause);
            MUTATE!((*newnode).args, (*tsc).args, *mut List);
            MUTATE!((*newnode).repeatable, (*tsc).repeatable, *mut Expr);
            return newnode as *mut Node;
        }
        NodeTag::T_TableFunc => {
            let tf = node as *mut TableFunc;
            FLATCOPY!(newnode, tf, TableFunc);
            MUTATE!((*newnode).ns_uris, (*tf).ns_uris, *mut List);
            MUTATE!((*newnode).docexpr, (*tf).docexpr, *mut Node);
            MUTATE!((*newnode).rowexpr, (*tf).rowexpr, *mut Node);
            MUTATE!((*newnode).colexprs, (*tf).colexprs, *mut List);
            MUTATE!((*newnode).coldefexprs, (*tf).coldefexprs, *mut List);
            MUTATE!((*newnode).colvalexprs, (*tf).colvalexprs, *mut List);
            MUTATE!((*newnode).passingvalexprs, (*tf).passingvalexprs, *mut List);
            return newnode as *mut Node;
        }
        _ => {
            elog!(ERROR, "unrecognized node type: {}", nodeTag(node) as c_int);
        }
    }
    /* can't get here, but keep compiler happy */
    null_mut()
}

/// `expression_tree_mutator(n, m, c)` (nodeFuncs.h macro): thin wrapper.
///
/// # Safety
/// `node` must be NULL or a valid node tree; `mutator` a valid callback.
#[inline]
pub unsafe fn expression_tree_mutator(
    node: *mut Node,
    mutator: tree_mutator_callback,
    context: *mut c_void,
) -> *mut Node {
    expression_tree_mutator_impl(node, mutator, context)
}

/*
 * query_tree_mutator --- initiate modification of a Query's expressions
 *
 * This routine exists just to reduce the number of places that need to know
 * where all the expression subtrees of a Query are.  Note it can be used
 * for starting a walk at top level of a Query regardless of whether the
 * mutator intends to descend into subqueries.  It is also useful for
 * descending into subqueries within a mutator.
 *
 * Some callers want to suppress mutating of certain items in the Query,
 * typically because they need to process them specially, or don't actually
 * want to recurse into subqueries.  This is supported by the flags argument,
 * which is the bitwise OR of flag values to suppress mutating of
 * indicated items.  (More flag bits may be added as needed.)
 *
 * Normally the top-level Query node itself is copied, but some callers want
 * it to be modified in-place; they must pass QTW_DONT_COPY_QUERY in flags.
 * All modified substructure is safely copied in any case.
 */
pub unsafe fn query_tree_mutator_impl(
    mut query: *mut Query,
    mutator: tree_mutator_callback,
    context: *mut c_void,
    flags: c_int,
) -> *mut Query {
    let m = mutator.unwrap();

    macro_rules! FLATCOPY {
        ($newnode:ident, $node:expr, $nodetype:ty) => {
            let $newnode: *mut $nodetype =
                palloc(core::mem::size_of::<$nodetype>()) as *mut $nodetype;
            core::ptr::copy_nonoverlapping($node as *const $nodetype, $newnode, 1);
        };
    }
    macro_rules! MUTATE {
        ($newfield:expr, $oldfield:expr, $fieldtype:ty) => {
            $newfield = m($oldfield as *mut Node, context) as $fieldtype
        };
    }

    Assert!(!query.is_null() && IsA!(query, T_Query));

    if (flags & QTW_DONT_COPY_QUERY) == 0 {
        FLATCOPY!(newquery, query, Query);
        query = newquery;
    }

    MUTATE!((*query).targetList, (*query).targetList, *mut List);
    MUTATE!((*query).withCheckOptions, (*query).withCheckOptions, *mut List);
    MUTATE!((*query).onConflict, (*query).onConflict, *mut OnConflictExpr);
    MUTATE!((*query).mergeActionList, (*query).mergeActionList, *mut List);
    MUTATE!((*query).mergeJoinCondition, (*query).mergeJoinCondition, *mut Node);
    MUTATE!((*query).returningList, (*query).returningList, *mut List);
    MUTATE!((*query).jointree, (*query).jointree, *mut FromExpr);
    MUTATE!((*query).setOperations, (*query).setOperations, *mut Node);
    MUTATE!((*query).havingQual, (*query).havingQual, *mut Node);
    MUTATE!((*query).limitOffset, (*query).limitOffset, *mut Node);
    MUTATE!((*query).limitCount, (*query).limitCount, *mut Node);

    /*
     * Most callers aren't interested in SortGroupClause nodes since those
     * don't contain actual expressions. However they do contain OIDs, which
     * may be of interest to some mutators.
     */

    if (flags & QTW_EXAMINE_SORTGROUP) != 0 {
        MUTATE!((*query).groupClause, (*query).groupClause, *mut List);
        MUTATE!((*query).windowClause, (*query).windowClause, *mut List);
        MUTATE!((*query).sortClause, (*query).sortClause, *mut List);
        MUTATE!((*query).distinctClause, (*query).distinctClause, *mut List);
    } else {
        /*
         * But we need to mutate the expressions under WindowClause nodes even
         * if we're not interested in SortGroupClause nodes.
         */
        let mut resultlist: *mut List;

        resultlist = NIL;
        foreach!(temp, (*query).windowClause, {
            let wc = lfirst_node!(WindowClause, T_WindowClause, current_cell!(temp));

            FLATCOPY!(newnode, wc, WindowClause);
            MUTATE!((*newnode).startOffset, (*wc).startOffset, *mut Node);
            MUTATE!((*newnode).endOffset, (*wc).endOffset, *mut Node);

            resultlist = lappend(resultlist, newnode as *mut c_void);
        });
        (*query).windowClause = resultlist;
    }

    /*
     * groupingSets and rowMarks are not mutated:
     *
     * groupingSets contain only ressortgroup refs (integers) which are
     * meaningless without the groupClause or tlist. Accordingly, any mutator
     * that needs to care about them needs to handle them itself in its Query
     * processing.
     *
     * rowMarks contains only rangetable indexes (and flags etc.) and
     * therefore should be handled at Query level similarly.
     */

    if (flags & QTW_IGNORE_CTE_SUBQUERIES) == 0 {
        MUTATE!((*query).cteList, (*query).cteList, *mut List);
    } else {
        /* else copy CTE list as-is */
        (*query).cteList = copyObject((*query).cteList);
    }
    (*query).rtable = range_table_mutator((*query).rtable, mutator, context, flags);
    query
}

/// `query_tree_mutator(q, m, c, f)` (nodeFuncs.h macro): thin wrapper.
///
/// # Safety
/// `query` must be a valid Query; `mutator` a valid callback.
#[inline]
pub unsafe fn query_tree_mutator(
    query: *mut Query,
    mutator: tree_mutator_callback,
    context: *mut c_void,
    flags: c_int,
) -> *mut Query {
    query_tree_mutator_impl(query, mutator, context, flags)
}

/*
 * range_table_mutator is just the part of query_tree_mutator that processes
 * a query's rangetable.  This is split out since it can be useful on
 * its own.
 */
pub unsafe fn range_table_mutator_impl(
    rtable: *mut List,
    mutator: tree_mutator_callback,
    context: *mut c_void,
    flags: c_int,
) -> *mut List {
    let m = mutator.unwrap();

    macro_rules! FLATCOPY {
        ($newnode:ident, $node:expr, $nodetype:ty) => {
            let $newnode: *mut $nodetype =
                palloc(core::mem::size_of::<$nodetype>()) as *mut $nodetype;
            core::ptr::copy_nonoverlapping($node as *const $nodetype, $newnode, 1);
        };
    }
    macro_rules! MUTATE {
        ($newfield:expr, $oldfield:expr, $fieldtype:ty) => {
            $newfield = m($oldfield as *mut Node, context) as $fieldtype
        };
    }

    let mut newrt: *mut List = NIL;

    foreach!(rt, rtable, {
        let rte = lfirst(current_cell!(rt)) as *mut RangeTblEntry;

        FLATCOPY!(newrte, rte, RangeTblEntry);
        match (*rte).rtekind {
            RTEKind::RTE_RELATION => {
                MUTATE!((*newrte).tablesample, (*rte).tablesample, *mut TableSampleClause);
                /* we don't bother to copy eref, aliases, etc; OK? */
            }
            RTEKind::RTE_SUBQUERY => {
                if (flags & QTW_IGNORE_RT_SUBQUERIES) == 0 {
                    MUTATE!((*newrte).subquery, (*rte).subquery, *mut Query);
                } else {
                    /* else, copy RT subqueries as-is */
                    (*newrte).subquery = copyObject((*rte).subquery);
                }
            }
            RTEKind::RTE_JOIN => {
                if (flags & QTW_IGNORE_JOINALIASES) == 0 {
                    MUTATE!((*newrte).joinaliasvars, (*rte).joinaliasvars, *mut List);
                } else {
                    /* else, copy join aliases as-is */
                    (*newrte).joinaliasvars = copyObject((*rte).joinaliasvars);
                }
            }
            RTEKind::RTE_FUNCTION => {
                MUTATE!((*newrte).functions, (*rte).functions, *mut List);
            }
            RTEKind::RTE_TABLEFUNC => {
                MUTATE!((*newrte).tablefunc, (*rte).tablefunc, *mut TableFunc);
            }
            RTEKind::RTE_VALUES => {
                MUTATE!((*newrte).values_lists, (*rte).values_lists, *mut List);
            }
            RTEKind::RTE_CTE | RTEKind::RTE_NAMEDTUPLESTORE | RTEKind::RTE_RESULT => {
                /* nothing to do */
            }
            RTEKind::RTE_GROUP => {
                if (flags & QTW_IGNORE_GROUPEXPRS) == 0 {
                    MUTATE!((*newrte).groupexprs, (*rte).groupexprs, *mut List);
                } else {
                    /* else, copy grouping exprs as-is */
                    (*newrte).groupexprs = copyObject((*rte).groupexprs);
                }
            }
        }
        MUTATE!((*newrte).securityQuals, (*rte).securityQuals, *mut List);
        newrt = lappend(newrt, newrte as *mut c_void);
    });
    newrt
}

/// `range_table_mutator(rt, m, c, f)` (nodeFuncs.h macro): thin wrapper.
///
/// # Safety
/// `rtable` must be NIL or a valid List of RangeTblEntry; `mutator` valid.
#[inline]
pub unsafe fn range_table_mutator(
    rtable: *mut List,
    mutator: tree_mutator_callback,
    context: *mut c_void,
    flags: c_int,
) -> *mut List {
    range_table_mutator_impl(rtable, mutator, context, flags)
}

/*
 * query_or_expression_tree_walker --- hybrid form
 *
 * This routine will invoke query_tree_walker if called on a Query node,
 * else will invoke the walker directly.  This is a useful way of starting
 * the recursion when the walker's normal change of state is not appropriate
 * for the outermost Query node.
 */
pub unsafe fn query_or_expression_tree_walker_impl(
    node: *mut Node,
    walker: tree_walker_callback,
    context: *mut c_void,
    flags: c_int,
) -> bool {
    if !node.is_null() && IsA!(node, T_Query) {
        query_tree_walker(node as *mut Query, walker, context, flags)
    } else {
        (walker.unwrap())(node, context)
    }
}

/// `query_or_expression_tree_walker(n, w, c, f)` (nodeFuncs.h macro): wrapper.
///
/// # Safety
/// `node` must be NULL or a valid node tree; `walker` a valid callback.
#[inline]
pub unsafe fn query_or_expression_tree_walker(
    node: *mut Node,
    walker: tree_walker_callback,
    context: *mut c_void,
    flags: c_int,
) -> bool {
    query_or_expression_tree_walker_impl(node, walker, context, flags)
}

/*
 * query_or_expression_tree_mutator --- hybrid form
 *
 * This routine will invoke query_tree_mutator if called on a Query node,
 * else will invoke the mutator directly.  This is a useful way of starting
 * the recursion when the mutator's normal change of state is not appropriate
 * for the outermost Query node.
 */
pub unsafe fn query_or_expression_tree_mutator_impl(
    node: *mut Node,
    mutator: tree_mutator_callback,
    context: *mut c_void,
    flags: c_int,
) -> *mut Node {
    if !node.is_null() && IsA!(node, T_Query) {
        query_tree_mutator(node as *mut Query, mutator, context, flags) as *mut Node
    } else {
        (mutator.unwrap())(node, context)
    }
}

/// `query_or_expression_tree_mutator(n, m, c, f)` (nodeFuncs.h macro): wrapper.
///
/// # Safety
/// `node` must be NULL or a valid node tree; `mutator` a valid callback.
#[inline]
pub unsafe fn query_or_expression_tree_mutator(
    node: *mut Node,
    mutator: tree_mutator_callback,
    context: *mut c_void,
    flags: c_int,
) -> *mut Node {
    query_or_expression_tree_mutator_impl(node, mutator, context, flags)
}

/*
 * raw_expression_tree_walker --- walk raw parse trees
 *
 * This has exactly the same API as expression_tree_walker, but instead of
 * walking post-analysis parse trees, it knows how to walk the node types
 * found in raw grammar output.  (There is not currently any need for a
 * combined walker, so we keep them separate in the name of efficiency.)
 * Unlike expression_tree_walker, there is no special rule about query
 * boundaries: we descend to everything that's possibly interesting.
 *
 * Currently, the node type coverage here extends only to DML statements
 * (SELECT/INSERT/UPDATE/DELETE/MERGE) and nodes that can appear in them,
 * because this is used mainly during analysis of CTEs, and only DML
 * statements can appear in CTEs.
 */
pub unsafe fn raw_expression_tree_walker_impl(
    node: *mut Node,
    walker: tree_walker_callback,
    context: *mut c_void,
) -> bool {
    let w = walker.unwrap();
    macro_rules! WALK {
        ($n:expr) => {
            w($n as *mut Node, context)
        };
    }

    /*
     * The walker has already visited the current node, and so we need only
     * recurse into any sub-nodes it has.
     */
    if node.is_null() {
        return false;
    }

    /* Guard against stack overflow due to overly complex expressions */
    check_stack_depth();

    match nodeTag(node) {
        NodeTag::T_JsonFormat
        | NodeTag::T_SetToDefault
        | NodeTag::T_CurrentOfExpr
        | NodeTag::T_SQLValueFunction
        | NodeTag::T_Integer
        | NodeTag::T_Float
        | NodeTag::T_Boolean
        | NodeTag::T_String
        | NodeTag::T_BitString
        | NodeTag::T_ParamRef
        | NodeTag::T_A_Const
        | NodeTag::T_A_Star
        | NodeTag::T_MergeSupportFunc
        | NodeTag::T_ReturningOption => {
            /* primitive node types with no subnodes */
        }
        NodeTag::T_Alias => {
            /* we assume the colnames list isn't interesting */
        }
        NodeTag::T_RangeVar => {
            return WALK!((*(node as *mut RangeVar)).alias);
        }
        NodeTag::T_GroupingFunc => {
            return WALK!((*(node as *mut GroupingFunc)).args);
        }
        NodeTag::T_SubLink => {
            let sublink = node as *mut SubLink;

            if WALK!((*sublink).testexpr) {
                return true;
            }
            /* we assume the operName is not interesting */
            if WALK!((*sublink).subselect) {
                return true;
            }
        }
        NodeTag::T_CaseExpr => {
            let caseexpr = node as *mut CaseExpr;

            if WALK!((*caseexpr).arg) {
                return true;
            }
            /* we assume walker doesn't care about CaseWhens, either */
            foreach!(temp, (*caseexpr).args, {
                let when = lfirst_node!(CaseWhen, T_CaseWhen, current_cell!(temp));

                if WALK!((*when).expr) {
                    return true;
                }
                if WALK!((*when).result) {
                    return true;
                }
            });
            if WALK!((*caseexpr).defresult) {
                return true;
            }
        }
        NodeTag::T_RowExpr => {
            /* Assume colnames isn't interesting */
            return WALK!((*(node as *mut RowExpr)).args);
        }
        NodeTag::T_CoalesceExpr => {
            return WALK!((*(node as *mut CoalesceExpr)).args);
        }
        NodeTag::T_MinMaxExpr => {
            return WALK!((*(node as *mut MinMaxExpr)).args);
        }
        NodeTag::T_XmlExpr => {
            let xexpr = node as *mut XmlExpr;

            if WALK!((*xexpr).named_args) {
                return true;
            }
            /* we assume walker doesn't care about arg_names */
            if WALK!((*xexpr).args) {
                return true;
            }
        }
        NodeTag::T_JsonReturning => {
            return WALK!((*(node as *mut JsonReturning)).format);
        }
        NodeTag::T_JsonValueExpr => {
            let jve = node as *mut JsonValueExpr;

            if WALK!((*jve).raw_expr) {
                return true;
            }
            if WALK!((*jve).formatted_expr) {
                return true;
            }
            if WALK!((*jve).format) {
                return true;
            }
        }
        NodeTag::T_JsonParseExpr => {
            let jpe = node as *mut JsonParseExpr;

            if WALK!((*jpe).expr) {
                return true;
            }
            if WALK!((*jpe).output) {
                return true;
            }
        }
        NodeTag::T_JsonScalarExpr => {
            let jse = node as *mut JsonScalarExpr;

            if WALK!((*jse).expr) {
                return true;
            }
            if WALK!((*jse).output) {
                return true;
            }
        }
        NodeTag::T_JsonSerializeExpr => {
            let jse = node as *mut JsonSerializeExpr;

            if WALK!((*jse).expr) {
                return true;
            }
            if WALK!((*jse).output) {
                return true;
            }
        }
        NodeTag::T_JsonConstructorExpr => {
            let ctor = node as *mut JsonConstructorExpr;

            if WALK!((*ctor).args) {
                return true;
            }
            if WALK!((*ctor).func) {
                return true;
            }
            if WALK!((*ctor).coercion) {
                return true;
            }
            if WALK!((*ctor).returning) {
                return true;
            }
        }
        NodeTag::T_JsonIsPredicate => {
            return WALK!((*(node as *mut JsonIsPredicate)).expr);
        }
        NodeTag::T_JsonArgument => {
            return WALK!((*(node as *mut JsonArgument)).val);
        }
        NodeTag::T_JsonFuncExpr => {
            let jfe = node as *mut JsonFuncExpr;

            if WALK!((*jfe).context_item) {
                return true;
            }
            if WALK!((*jfe).pathspec) {
                return true;
            }
            if WALK!((*jfe).passing) {
                return true;
            }
            if WALK!((*jfe).output) {
                return true;
            }
            if WALK!((*jfe).on_empty) {
                return true;
            }
            if WALK!((*jfe).on_error) {
                return true;
            }
        }
        NodeTag::T_JsonBehavior => {
            let jb = node as *mut JsonBehavior;

            if WALK!((*jb).expr) {
                return true;
            }
        }
        NodeTag::T_JsonTable => {
            let jt = node as *mut JsonTable;

            if WALK!((*jt).context_item) {
                return true;
            }
            if WALK!((*jt).pathspec) {
                return true;
            }
            if WALK!((*jt).passing) {
                return true;
            }
            if WALK!((*jt).columns) {
                return true;
            }
            if WALK!((*jt).on_error) {
                return true;
            }
        }
        NodeTag::T_JsonTableColumn => {
            let jtc = node as *mut JsonTableColumn;

            if WALK!((*jtc).typeName) {
                return true;
            }
            if WALK!((*jtc).on_empty) {
                return true;
            }
            if WALK!((*jtc).on_error) {
                return true;
            }
            if WALK!((*jtc).columns) {
                return true;
            }
        }
        NodeTag::T_JsonTablePathSpec => {
            return WALK!((*(node as *mut JsonTablePathSpec)).string);
        }
        NodeTag::T_NullTest => {
            return WALK!((*(node as *mut NullTest)).arg);
        }
        NodeTag::T_BooleanTest => {
            return WALK!((*(node as *mut BooleanTest)).arg);
        }
        NodeTag::T_JoinExpr => {
            let join = node as *mut JoinExpr;

            if WALK!((*join).larg) {
                return true;
            }
            if WALK!((*join).rarg) {
                return true;
            }
            if WALK!((*join).quals) {
                return true;
            }
            if WALK!((*join).alias) {
                return true;
            }
            /* using list is deemed uninteresting */
        }
        NodeTag::T_IntoClause => {
            let into = node as *mut IntoClause;

            if WALK!((*into).rel) {
                return true;
            }
            /* colNames, options are deemed uninteresting */
            /* viewQuery should be null in raw parsetree, but check it */
            if WALK!((*into).viewQuery) {
                return true;
            }
        }
        NodeTag::T_List => {
            foreach!(temp, node as *mut List, {
                if WALK!(lfirst(current_cell!(temp)) as *mut Node) {
                    return true;
                }
            });
        }
        NodeTag::T_InsertStmt => {
            let stmt = node as *mut InsertStmt;

            if WALK!((*stmt).relation) {
                return true;
            }
            if WALK!((*stmt).cols) {
                return true;
            }
            if WALK!((*stmt).selectStmt) {
                return true;
            }
            if WALK!((*stmt).onConflictClause) {
                return true;
            }
            if WALK!((*stmt).returningClause) {
                return true;
            }
            if WALK!((*stmt).withClause) {
                return true;
            }
        }
        NodeTag::T_DeleteStmt => {
            let stmt = node as *mut DeleteStmt;

            if WALK!((*stmt).relation) {
                return true;
            }
            if WALK!((*stmt).usingClause) {
                return true;
            }
            if WALK!((*stmt).whereClause) {
                return true;
            }
            if WALK!((*stmt).returningClause) {
                return true;
            }
            if WALK!((*stmt).withClause) {
                return true;
            }
        }
        NodeTag::T_UpdateStmt => {
            let stmt = node as *mut UpdateStmt;

            if WALK!((*stmt).relation) {
                return true;
            }
            if WALK!((*stmt).targetList) {
                return true;
            }
            if WALK!((*stmt).whereClause) {
                return true;
            }
            if WALK!((*stmt).fromClause) {
                return true;
            }
            if WALK!((*stmt).returningClause) {
                return true;
            }
            if WALK!((*stmt).withClause) {
                return true;
            }
        }
        NodeTag::T_MergeStmt => {
            let stmt = node as *mut MergeStmt;

            if WALK!((*stmt).relation) {
                return true;
            }
            if WALK!((*stmt).sourceRelation) {
                return true;
            }
            if WALK!((*stmt).joinCondition) {
                return true;
            }
            if WALK!((*stmt).mergeWhenClauses) {
                return true;
            }
            if WALK!((*stmt).returningClause) {
                return true;
            }
            if WALK!((*stmt).withClause) {
                return true;
            }
        }
        NodeTag::T_MergeWhenClause => {
            let mergeWhenClause = node as *mut MergeWhenClause;

            if WALK!((*mergeWhenClause).condition) {
                return true;
            }
            if WALK!((*mergeWhenClause).targetList) {
                return true;
            }
            if WALK!((*mergeWhenClause).values) {
                return true;
            }
        }
        NodeTag::T_ReturningClause => {
            let returning = node as *mut ReturningClause;

            if WALK!((*returning).options) {
                return true;
            }
            if WALK!((*returning).exprs) {
                return true;
            }
        }
        NodeTag::T_SelectStmt => {
            let stmt = node as *mut SelectStmt;

            if WALK!((*stmt).distinctClause) {
                return true;
            }
            if WALK!((*stmt).intoClause) {
                return true;
            }
            if WALK!((*stmt).targetList) {
                return true;
            }
            if WALK!((*stmt).fromClause) {
                return true;
            }
            if WALK!((*stmt).whereClause) {
                return true;
            }
            if WALK!((*stmt).groupClause) {
                return true;
            }
            if WALK!((*stmt).havingClause) {
                return true;
            }
            if WALK!((*stmt).windowClause) {
                return true;
            }
            if WALK!((*stmt).valuesLists) {
                return true;
            }
            if WALK!((*stmt).sortClause) {
                return true;
            }
            if WALK!((*stmt).limitOffset) {
                return true;
            }
            if WALK!((*stmt).limitCount) {
                return true;
            }
            if WALK!((*stmt).lockingClause) {
                return true;
            }
            if WALK!((*stmt).withClause) {
                return true;
            }
            if WALK!((*stmt).larg) {
                return true;
            }
            if WALK!((*stmt).rarg) {
                return true;
            }
        }
        NodeTag::T_PLAssignStmt => {
            let stmt = node as *mut PLAssignStmt;

            if WALK!((*stmt).indirection) {
                return true;
            }
            if WALK!((*stmt).val) {
                return true;
            }
        }
        NodeTag::T_A_Expr => {
            let expr = node as *mut A_Expr;

            if WALK!((*expr).lexpr) {
                return true;
            }
            if WALK!((*expr).rexpr) {
                return true;
            }
            /* operator name is deemed uninteresting */
        }
        NodeTag::T_BoolExpr => {
            let expr = node as *mut BoolExpr;

            if WALK!((*expr).args) {
                return true;
            }
        }
        NodeTag::T_ColumnRef => {
            /* we assume the fields contain nothing interesting */
        }
        NodeTag::T_FuncCall => {
            let fcall = node as *mut FuncCall;

            if WALK!((*fcall).args) {
                return true;
            }
            if WALK!((*fcall).agg_order) {
                return true;
            }
            if WALK!((*fcall).agg_filter) {
                return true;
            }
            if WALK!((*fcall).over) {
                return true;
            }
            /* function name is deemed uninteresting */
        }
        NodeTag::T_NamedArgExpr => {
            return WALK!((*(node as *mut NamedArgExpr)).arg);
        }
        NodeTag::T_A_Indices => {
            let indices = node as *mut A_Indices;

            if WALK!((*indices).lidx) {
                return true;
            }
            if WALK!((*indices).uidx) {
                return true;
            }
        }
        NodeTag::T_A_Indirection => {
            let indir = node as *mut A_Indirection;

            if WALK!((*indir).arg) {
                return true;
            }
            if WALK!((*indir).indirection) {
                return true;
            }
        }
        NodeTag::T_A_ArrayExpr => {
            return WALK!((*(node as *mut A_ArrayExpr)).elements);
        }
        NodeTag::T_ResTarget => {
            let rt = node as *mut ResTarget;

            if WALK!((*rt).indirection) {
                return true;
            }
            if WALK!((*rt).val) {
                return true;
            }
        }
        NodeTag::T_MultiAssignRef => {
            return WALK!((*(node as *mut MultiAssignRef)).source);
        }
        NodeTag::T_TypeCast => {
            let tc = node as *mut TypeCast;

            if WALK!((*tc).arg) {
                return true;
            }
            if WALK!((*tc).typeName) {
                return true;
            }
        }
        NodeTag::T_CollateClause => {
            return WALK!((*(node as *mut CollateClause)).arg);
        }
        NodeTag::T_SortBy => {
            return WALK!((*(node as *mut SortBy)).node);
        }
        NodeTag::T_WindowDef => {
            let wd = node as *mut WindowDef;

            if WALK!((*wd).partitionClause) {
                return true;
            }
            if WALK!((*wd).orderClause) {
                return true;
            }
            if WALK!((*wd).startOffset) {
                return true;
            }
            if WALK!((*wd).endOffset) {
                return true;
            }
        }
        NodeTag::T_RangeSubselect => {
            let rs = node as *mut RangeSubselect;

            if WALK!((*rs).subquery) {
                return true;
            }
            if WALK!((*rs).alias) {
                return true;
            }
        }
        NodeTag::T_RangeFunction => {
            let rf = node as *mut RangeFunction;

            if WALK!((*rf).functions) {
                return true;
            }
            if WALK!((*rf).alias) {
                return true;
            }
            if WALK!((*rf).coldeflist) {
                return true;
            }
        }
        NodeTag::T_RangeTableSample => {
            let rts = node as *mut RangeTableSample;

            if WALK!((*rts).relation) {
                return true;
            }
            /* method name is deemed uninteresting */
            if WALK!((*rts).args) {
                return true;
            }
            if WALK!((*rts).repeatable) {
                return true;
            }
        }
        NodeTag::T_RangeTableFunc => {
            let rtf = node as *mut RangeTableFunc;

            if WALK!((*rtf).docexpr) {
                return true;
            }
            if WALK!((*rtf).rowexpr) {
                return true;
            }
            if WALK!((*rtf).namespaces) {
                return true;
            }
            if WALK!((*rtf).columns) {
                return true;
            }
            if WALK!((*rtf).alias) {
                return true;
            }
        }
        NodeTag::T_RangeTableFuncCol => {
            let rtfc = node as *mut RangeTableFuncCol;

            if WALK!((*rtfc).colexpr) {
                return true;
            }
            if WALK!((*rtfc).coldefexpr) {
                return true;
            }
        }
        NodeTag::T_TypeName => {
            let tn = node as *mut TypeName;

            if WALK!((*tn).typmods) {
                return true;
            }
            if WALK!((*tn).arrayBounds) {
                return true;
            }
            /* type name itself is deemed uninteresting */
        }
        NodeTag::T_ColumnDef => {
            let coldef = node as *mut ColumnDef;

            if WALK!((*coldef).typeName) {
                return true;
            }
            if WALK!((*coldef).raw_default) {
                return true;
            }
            if WALK!((*coldef).collClause) {
                return true;
            }
            /* for now, constraints are ignored */
        }
        NodeTag::T_IndexElem => {
            let indelem = node as *mut IndexElem;

            if WALK!((*indelem).expr) {
                return true;
            }
            /* collation and opclass names are deemed uninteresting */
        }
        NodeTag::T_GroupingSet => {
            return WALK!((*(node as *mut GroupingSet)).content);
        }
        NodeTag::T_LockingClause => {
            return WALK!((*(node as *mut LockingClause)).lockedRels);
        }
        NodeTag::T_XmlSerialize => {
            let xs = node as *mut XmlSerialize;

            if WALK!((*xs).expr) {
                return true;
            }
            if WALK!((*xs).typeName) {
                return true;
            }
        }
        NodeTag::T_WithClause => {
            return WALK!((*(node as *mut WithClause)).ctes);
        }
        NodeTag::T_InferClause => {
            let stmt = node as *mut InferClause;

            if WALK!((*stmt).indexElems) {
                return true;
            }
            if WALK!((*stmt).whereClause) {
                return true;
            }
        }
        NodeTag::T_OnConflictClause => {
            let stmt = node as *mut OnConflictClause;

            if WALK!((*stmt).infer) {
                return true;
            }
            if WALK!((*stmt).targetList) {
                return true;
            }
            if WALK!((*stmt).whereClause) {
                return true;
            }
        }
        NodeTag::T_CommonTableExpr => {
            /* search_clause and cycle_clause are not interesting here */
            return WALK!((*(node as *mut CommonTableExpr)).ctequery);
        }
        NodeTag::T_JsonOutput => {
            let out = node as *mut JsonOutput;

            if WALK!((*out).typeName) {
                return true;
            }
            if WALK!((*out).returning) {
                return true;
            }
        }
        NodeTag::T_JsonKeyValue => {
            let jkv = node as *mut JsonKeyValue;

            if WALK!((*jkv).key) {
                return true;
            }
            if WALK!((*jkv).value) {
                return true;
            }
        }
        NodeTag::T_JsonObjectConstructor => {
            let joc = node as *mut JsonObjectConstructor;

            if WALK!((*joc).output) {
                return true;
            }
            if WALK!((*joc).exprs) {
                return true;
            }
        }
        NodeTag::T_JsonArrayConstructor => {
            let jac = node as *mut JsonArrayConstructor;

            if WALK!((*jac).output) {
                return true;
            }
            if WALK!((*jac).exprs) {
                return true;
            }
        }
        NodeTag::T_JsonAggConstructor => {
            let ctor = node as *mut JsonAggConstructor;

            if WALK!((*ctor).output) {
                return true;
            }
            if WALK!((*ctor).agg_order) {
                return true;
            }
            if WALK!((*ctor).agg_filter) {
                return true;
            }
            if WALK!((*ctor).over) {
                return true;
            }
        }
        NodeTag::T_JsonObjectAgg => {
            let joa = node as *mut JsonObjectAgg;

            if WALK!((*joa).constructor) {
                return true;
            }
            if WALK!((*joa).arg) {
                return true;
            }
        }
        NodeTag::T_JsonArrayAgg => {
            let jaa = node as *mut JsonArrayAgg;

            if WALK!((*jaa).constructor) {
                return true;
            }
            if WALK!((*jaa).arg) {
                return true;
            }
        }
        NodeTag::T_JsonArrayQueryConstructor => {
            let jaqc = node as *mut JsonArrayQueryConstructor;

            if WALK!((*jaqc).output) {
                return true;
            }
            if WALK!((*jaqc).query) {
                return true;
            }
        }
        _ => {
            elog!(ERROR, "unrecognized node type: {}", nodeTag(node) as c_int);
        }
    }
    false
}

/// `raw_expression_tree_walker(n, w, c)` (nodeFuncs.h macro): thin wrapper.
///
/// # Safety
/// `node` must be NULL or a valid raw parse tree; `walker` a valid callback.
#[inline]
pub unsafe fn raw_expression_tree_walker(
    node: *mut Node,
    walker: tree_walker_callback,
    context: *mut c_void,
) -> bool {
    raw_expression_tree_walker_impl(node, walker, context)
}

/*
 * planstate_tree_walker --- walk plan state trees
 *
 * The walker has already visited the current node, and so we need only
 * recurse into any sub-nodes it has.
 */
pub unsafe fn planstate_tree_walker_impl(
    planstate: *mut PlanState,
    walker: planstate_tree_walker_callback,
    context: *mut c_void,
) -> bool {
    let plan: *mut Plan = (*planstate).plan;

    let w = walker.unwrap();
    /* We don't need implicit coercions to Node here */
    macro_rules! PSWALK {
        ($n:expr) => {
            w($n, context)
        };
    }

    /* Guard against stack overflow due to overly complex plan trees */
    check_stack_depth();

    /* initPlan-s */
    if planstate_walk_subplans((*planstate).initPlan, walker, context) {
        return true;
    }

    /* lefttree */
    if !outerPlanState(planstate).is_null() {
        if PSWALK!(outerPlanState(planstate)) {
            return true;
        }
    }

    /* righttree */
    if !innerPlanState(planstate).is_null() {
        if PSWALK!(innerPlanState(planstate)) {
            return true;
        }
    }

    /* special child plans */
    match nodeTag(plan) {
        NodeTag::T_Append => {
            if planstate_walk_members(
                (*(planstate as *mut AppendState)).appendplans,
                (*(planstate as *mut AppendState)).as_nplans,
                walker,
                context,
            ) {
                return true;
            }
        }
        NodeTag::T_MergeAppend => {
            if planstate_walk_members(
                (*(planstate as *mut MergeAppendState)).mergeplans,
                (*(planstate as *mut MergeAppendState)).ms_nplans,
                walker,
                context,
            ) {
                return true;
            }
        }
        NodeTag::T_BitmapAnd => {
            if planstate_walk_members(
                (*(planstate as *mut BitmapAndState)).bitmapplans,
                (*(planstate as *mut BitmapAndState)).nplans,
                walker,
                context,
            ) {
                return true;
            }
        }
        NodeTag::T_BitmapOr => {
            if planstate_walk_members(
                (*(planstate as *mut BitmapOrState)).bitmapplans,
                (*(planstate as *mut BitmapOrState)).nplans,
                walker,
                context,
            ) {
                return true;
            }
        }
        NodeTag::T_SubqueryScan => {
            if PSWALK!((*(planstate as *mut SubqueryScanState)).subplan) {
                return true;
            }
        }
        NodeTag::T_CustomScan => {
            foreach!(lc, (*(planstate as *mut CustomScanState)).custom_ps, {
                if PSWALK!(lfirst(current_cell!(lc)) as *mut PlanState) {
                    return true;
                }
            });
        }
        _ => {}
    }

    /* subPlan-s */
    if planstate_walk_subplans((*planstate).subPlan, walker, context) {
        return true;
    }

    false
}

/// `planstate_tree_walker(ps, w, c)` (nodeFuncs.h macro): thin wrapper.
///
/// # Safety
/// `planstate` must be a valid PlanState tree; `walker` a valid callback.
#[inline]
pub unsafe fn planstate_tree_walker(
    planstate: *mut PlanState,
    walker: planstate_tree_walker_callback,
    context: *mut c_void,
) -> bool {
    planstate_tree_walker_impl(planstate, walker, context)
}

/*
 * Walk a list of SubPlans (or initPlans, which also use SubPlan nodes).
 */
unsafe fn planstate_walk_subplans(
    plans: *mut List,
    walker: planstate_tree_walker_callback,
    context: *mut c_void,
) -> bool {
    let w = walker.unwrap();
    macro_rules! PSWALK {
        ($n:expr) => {
            w($n, context)
        };
    }

    foreach!(lc, plans, {
        let sps = lfirst_node!(SubPlanState, T_SubPlanState, current_cell!(lc));

        if PSWALK!((*sps).planstate) {
            return true;
        }
    });

    false
}

/*
 * Walk the constituent plans of a ModifyTable, Append, MergeAppend,
 * BitmapAnd, or BitmapOr node.
 */
unsafe fn planstate_walk_members(
    planstates: *mut *mut PlanState,
    nplans: c_int,
    walker: planstate_tree_walker_callback,
    context: *mut c_void,
) -> bool {
    let w = walker.unwrap();
    macro_rules! PSWALK {
        ($n:expr) => {
            w($n, context)
        };
    }

    let mut j: c_int = 0;
    while j < nplans {
        if PSWALK!(*planstates.add(j as usize)) {
            return true;
        }
        j += 1;
    }

    false
}
