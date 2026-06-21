//! parser/parse_node.c - various routines that make nodes for querytrees
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

use core::mem::size_of;

use crate::{
    boolVal, current_cell, foreach, intVal, lfirst_node, makeNode, strVal, DirectFunctionCall3,
};

use crate::access::attnum::AttrNumber;
use crate::access::htup_details::MaxTupleAttributeNumber;
// int32/int64/FLOAT8PASSBYVAL come from crate::c::* via the prelude.
use crate::nodes::makefuncs::makeConst;
use crate::nodes::miscnodes::ErrorSaveContext;
use crate::nodes::nodeFuncs::exprLocation;
use crate::nodes::nodes::{nodeTag, Node, NodeTag};
use crate::nodes::parsenodes::{A_Const, A_Indices};
use crate::nodes::pg_list::List;
use crate::nodes::primnodes::{Const, Expr, SubscriptingRef};
use crate::nodes::subscripting::SubscriptRoutines;
// Oid/InvalidOid come from crate::postgres_ext::* via the prelude.

// ---------------------------------------------------------------------------
// ParseState / ParseNamespaceItem / ParseNamespaceColumn / ParseCallbackState
//
// parse_node.c is the canonical home for these structs (parser/parse_node.h);
// other files carry partial stubs.  We define the REAL structs here.
// ---------------------------------------------------------------------------

pub type Index = c_uint;
pub type ParseLoc = c_int;

// utils/relcache.h: Relation is *mut RelationData.  RelationData is not yet
// ported in a way that's stable for parser use; keep it opaque here.
#[allow(non_camel_case_types)]
pub type Relation = *mut c_void;

// utils/queryenvironment.h: QueryEnvironment (opaque).
#[allow(non_camel_case_types)]
pub type QueryEnvironment = c_void;

/* parser/parse_node.h: ParseExprKind */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ParseExprKind {
    EXPR_KIND_NONE = 0,            /* "not in an expression" */
    EXPR_KIND_OTHER,              /* reserved for extensions */
    EXPR_KIND_JOIN_ON,           /* JOIN ON */
    EXPR_KIND_JOIN_USING,        /* JOIN USING */
    EXPR_KIND_FROM_SUBSELECT,    /* sub-SELECT in FROM clause */
    EXPR_KIND_FROM_FUNCTION,     /* function in FROM clause */
    EXPR_KIND_WHERE,             /* WHERE */
    EXPR_KIND_HAVING,            /* HAVING */
    EXPR_KIND_FILTER,            /* FILTER */
    EXPR_KIND_WINDOW_PARTITION,  /* window definition PARTITION BY */
    EXPR_KIND_WINDOW_ORDER,      /* window definition ORDER BY */
    EXPR_KIND_WINDOW_FRAME_RANGE, /* window frame clause with RANGE */
    EXPR_KIND_WINDOW_FRAME_ROWS,  /* window frame clause with ROWS */
    EXPR_KIND_WINDOW_FRAME_GROUPS, /* window frame clause with GROUPS */
    EXPR_KIND_SELECT_TARGET,     /* SELECT target list item */
    EXPR_KIND_INSERT_TARGET,     /* INSERT target list item */
    EXPR_KIND_UPDATE_SOURCE,     /* UPDATE assignment source item */
    EXPR_KIND_UPDATE_TARGET,     /* UPDATE assignment target item */
    EXPR_KIND_MERGE_WHEN,        /* MERGE WHEN [NOT] MATCHED condition */
    EXPR_KIND_GROUP_BY,          /* GROUP BY */
    EXPR_KIND_ORDER_BY,          /* ORDER BY */
    EXPR_KIND_DISTINCT_ON,       /* DISTINCT ON */
    EXPR_KIND_LIMIT,             /* LIMIT */
    EXPR_KIND_OFFSET,            /* OFFSET */
    EXPR_KIND_RETURNING,         /* RETURNING in INSERT/UPDATE/DELETE */
    EXPR_KIND_MERGE_RETURNING,   /* RETURNING in MERGE */
    EXPR_KIND_VALUES,            /* VALUES */
    EXPR_KIND_VALUES_SINGLE,     /* single-row VALUES (in INSERT only) */
    EXPR_KIND_CHECK_CONSTRAINT,  /* CHECK constraint for a table */
    EXPR_KIND_DOMAIN_CHECK,      /* CHECK constraint for a domain */
    EXPR_KIND_COLUMN_DEFAULT,    /* default value for a table column */
    EXPR_KIND_FUNCTION_DEFAULT,  /* default parameter value for function */
    EXPR_KIND_INDEX_EXPRESSION,  /* index expression */
    EXPR_KIND_INDEX_PREDICATE,   /* index predicate */
    EXPR_KIND_STATS_EXPRESSION,  /* extended statistics expression */
    EXPR_KIND_ALTER_COL_TRANSFORM, /* transform expr in ALTER COLUMN TYPE */
    EXPR_KIND_EXECUTE_PARAMETER, /* parameter value in EXECUTE */
    EXPR_KIND_TRIGGER_WHEN,      /* WHEN condition in CREATE TRIGGER */
    EXPR_KIND_POLICY,            /* USING or WITH CHECK expr in policy */
    EXPR_KIND_PARTITION_BOUND,   /* partition bound expression */
    EXPR_KIND_PARTITION_EXPRESSION, /* PARTITION BY expression */
    EXPR_KIND_CALL_ARGUMENT,     /* procedure argument in CALL */
    EXPR_KIND_COPY_WHERE,        /* WHERE condition in COPY FROM */
    EXPR_KIND_GENERATED_COLUMN,  /* generation expression for a column */
    EXPR_KIND_CYCLE_MARK,        /* cycle mark value */
}
pub use ParseExprKind::*;

/*
 * Function signatures for parser hooks.  These reference ColumnRef/ParamRef/
 * Param, which are defined in parsenodes/primnodes; we keep the argument
 * pointers as their concrete types where available, *mut c_void otherwise.
 */
pub type PreParseColumnRefHook =
    Option<unsafe fn(pstate: *mut ParseState, cref: *mut c_void) -> *mut Node>;
pub type PostParseColumnRefHook =
    Option<unsafe fn(pstate: *mut ParseState, cref: *mut c_void, var: *mut Node) -> *mut Node>;
pub type ParseParamRefHook =
    Option<unsafe fn(pstate: *mut ParseState, pref: *mut c_void) -> *mut Node>;
pub type CoerceParamHook = Option<
    unsafe fn(
        pstate: *mut ParseState,
        param: *mut c_void,
        targetTypeId: Oid,
        targetTypeMod: int32,
        location: c_int,
    ) -> *mut Node,
>;

/*
 * State information used during parse analysis
 */
#[repr(C)]
pub struct ParseState {
    pub parentParseState: *mut ParseState, /* stack link */
    pub p_sourcetext: *const c_char,       /* source text, or NULL if not available */
    pub p_rtable: *mut List,               /* range table so far */
    pub p_rteperminfos: *mut List,         /* RTEPermissionInfo per RTE_RELATION */
    pub p_joinexprs: *mut List,            /* JoinExprs for RTE_JOIN entries */
    pub p_nullingrels: *mut List,          /* Bitmapsets showing nulling outer joins */
    pub p_joinlist: *mut List,             /* join items so far (-> FromExpr fromlist) */
    pub p_namespace: *mut List,            /* currently-referenceable RTEs */
    pub p_lateral_active: bool,            /* p_lateral_only items visible? */
    pub p_ctenamespace: *mut List,         /* current namespace for common table exprs */
    pub p_future_ctes: *mut List,          /* common table exprs not yet in namespace */
    pub p_parent_cte: *mut c_void,         /* CommonTableExpr containing this query */
    pub p_target_relation: Relation,       /* INSERT/UPDATE/DELETE/MERGE target rel */
    pub p_target_nsitem: *mut ParseNamespaceItem, /* target rel's NSItem, or NULL */
    pub p_grouping_nsitem: *mut ParseNamespaceItem, /* NSItem for grouping, or NULL */
    pub p_is_insert: bool,                 /* process assignment like INSERT not UPDATE */
    pub p_windowdefs: *mut List,           /* raw representations of window clauses */
    pub p_expr_kind: ParseExprKind,        /* what kind of expression we're parsing */
    pub p_next_resno: c_int,               /* next targetlist resno to assign */
    pub p_multiassign_exprs: *mut List,    /* junk tlist entries for multiassign */
    pub p_locking_clause: *mut List,       /* raw FOR UPDATE/FOR SHARE info */
    pub p_locked_from_parent: bool,        /* parent FOR UPDATE/SHARE on this subquery */
    pub p_resolve_unknowns: bool,          /* resolve unknown-type SELECT outputs as text */

    pub p_queryEnv: *mut QueryEnvironment, /* curr env, incl refs to enclosing env */

    /* Flags telling about things found in the query: */
    pub p_hasAggs: bool,
    pub p_hasWindowFuncs: bool,
    pub p_hasTargetSRFs: bool,
    pub p_hasSubLinks: bool,
    pub p_hasModifyingCTE: bool,

    pub p_last_srf: *mut Node, /* most recent set-returning func/op found */

    /*
     * Optional hook functions for parser callbacks.  These are null unless
     * set up by the caller of make_parsestate.
     */
    pub p_pre_columnref_hook: PreParseColumnRefHook,
    pub p_post_columnref_hook: PostParseColumnRefHook,
    pub p_paramref_hook: ParseParamRefHook,
    pub p_coerce_param_hook: CoerceParamHook,
    pub p_ref_hook_state: *mut c_void, /* common passthrough link for above */
}

/*
 * An element of a namespace list.
 */
#[repr(C)]
pub struct ParseNamespaceItem {
    pub p_names: *mut c_void,      /* Alias: Table and column names */
    pub p_rte: *mut c_void,        /* RangeTblEntry: the relation's rangetable entry */
    pub p_rtindex: c_int,          /* The relation's index in the rangetable */
    pub p_perminfo: *mut c_void,   /* RTEPermissionInfo: relation's rteperminfos entry */
    pub p_nscolumns: *mut ParseNamespaceColumn, /* per-column data */
    pub p_rel_visible: bool,       /* Relation name is visible? */
    pub p_cols_visible: bool,      /* Column names visible as unqualified refs? */
    pub p_lateral_only: bool,      /* Is only visible to LATERAL expressions? */
    pub p_lateral_ok: bool,        /* If so, does join type allow use? */
    pub p_returning_type: c_int,   /* VarReturningType: Is OLD/NEW for RETURNING? */
}

/*
 * Data about one column of a ParseNamespaceItem.
 */
#[repr(C)]
pub struct ParseNamespaceColumn {
    pub p_varno: Index,                /* rangetable index */
    pub p_varattno: AttrNumber,        /* attribute number of the column */
    pub p_vartype: Oid,                /* pg_type OID */
    pub p_vartypmod: int32,            /* type modifier value */
    pub p_varcollid: Oid,              /* OID of collation, or InvalidOid */
    pub p_varreturningtype: c_int,     /* VarReturningType: for RETURNING OLD/NEW */
    pub p_varnosyn: Index,             /* rangetable index of syntactic referent */
    pub p_varattnosyn: AttrNumber,     /* attribute number of syntactic referent */
    pub p_dontexpand: bool,            /* not included in star expansion */
}

/* utils/elog.h: ErrorContextCallback */
#[repr(C)]
pub struct ErrorContextCallback {
    pub previous: *mut ErrorContextCallback,
    pub callback: Option<unsafe extern "C" fn(arg: *mut c_void)>,
    pub arg: *mut c_void,
}

/* Support for parser_errposition_callback function */
#[repr(C)]
pub struct ParseCallbackState {
    pub pstate: *mut ParseState,
    pub location: c_int,
    pub errcallback: ErrorContextCallback,
}

// ---------------------------------------------------------------------------
// Stubs for unported callees.
// ---------------------------------------------------------------------------

// utils/elog.h: error_context_stack -- the process-global #[no_mangle] symbol
// defined in utils/error/elog_impl.rs.  Must be the SAME storage errfinish walks,
// or parser errposition callbacks never fire.
extern "C" {
    static mut error_context_stack: *mut ErrorContextCallback;
}

// utils/errcodes.h (not yet ported).
const ERRCODE_TOO_MANY_COLUMNS: c_int = 0;
const ERRCODE_DATATYPE_MISMATCH: c_int = 0;
const ERRCODE_QUERY_CANCELED: c_int = 0;

// storage/lockdefs.h: NoLock.
const NoLock: c_int = 0;

// catalog/pg_type.h OIDs (not yet ported as a Rust module).
const UNKNOWNOID: Oid = 705;
const INT4OID: Oid = 23;
const INT8OID: Oid = 20;
const NUMERICOID: Oid = 1700;
const BOOLOID: Oid = 16;
const BITOID: Oid = 1560;
const INT2VECTOROID: Oid = 22;
const INT2ARRAYOID: Oid = 1005;
const OIDVECTOROID: Oid = 30;
const OIDARRAYOID: Oid = 1028;

// FLOAT8PASSBYVAL comes from crate::c::* via the prelude.

#[inline]
fn OidIsValid(objectId: Oid) -> bool {
    objectId != InvalidOid
}

// utils/lsyscache.h: getBaseTypeAndTypmod (not yet ported).
unsafe fn getBaseTypeAndTypmod(typid: Oid, _typmod: *mut int32) -> Oid {
    crate::utils::cache::lsyscache::getBaseTypeAndTypmod(typid, _typmod as _)
}

// utils/typcache.h: getSubscriptingRoutines (not yet ported).
unsafe fn getSubscriptingRoutines(
    _containerType: Oid,
    _typelem: *mut Oid,
) -> *const SubscriptRoutines {
    crate::utils::cache::lsyscache::getSubscriptingRoutines(_containerType, _typelem) as _
}

// utils/builtins.h: format_type_be (not yet ported).
unsafe fn format_type_be(_type_oid: Oid) -> *mut c_char {
    crate::utils::adt::format_type::format_type_be(_type_oid as _) as _
}

// mb/pg_wchar.h: pg_mbstrlen_with_len (not yet ported).
unsafe fn pg_mbstrlen_with_len(_mbstr: *const c_char, _limit: c_int) -> c_int {
    crate::utils::mb::mbutils::pg_mbstrlen_with_len(_mbstr as _, _limit as _) as _
}

unsafe fn pg_strtoint64_safe(_s: *const c_char, _escontext: *mut Node) -> int64 {
    crate::utils::builtins::pg_strtoint64_safe(_s, _escontext as _)
}

unsafe fn table_close(_relation: Relation, _lockmode: c_int) {
    crate::access::table::table::table_close(_relation as _, _lockmode as _)
}

unsafe fn errposition(_cursorpos: c_int) -> c_int {
    crate::utils::error::elog_impl::errposition(_cursorpos)
}
unsafe fn geterrcode() -> c_int {
    crate::utils::error::elog_impl::geterrcode()
}

// fmgr-callable builtins invoked via DirectFunctionCall3 (not yet ported).
unsafe fn numeric_in(_fcinfo: crate::utils::fmgr::FunctionCallInfo) -> Datum {
    crate::utils::adt::numeric::numeric_in(_fcinfo as _) as _
}
unsafe fn bit_in(_fcinfo: crate::utils::fmgr::FunctionCallInfo) -> Datum {
    crate::utils::adt::varbit::bit_in(_fcinfo as _) as _
}

/*
 * make_parsestate
 *		Allocate and initialize a new ParseState.
 *
 * Caller should eventually release the ParseState via free_parsestate().
 */
pub unsafe fn make_parsestate(parentParseState: *mut ParseState) -> *mut ParseState {
    let pstate: *mut ParseState = palloc0(size_of::<ParseState>()) as *mut ParseState;

    (*pstate).parentParseState = parentParseState;

    /* Fill in fields that don't start at null/false/zero */
    (*pstate).p_next_resno = 1;
    (*pstate).p_resolve_unknowns = true;

    if !parentParseState.is_null() {
        (*pstate).p_sourcetext = (*parentParseState).p_sourcetext;
        /* all hooks are copied from parent */
        (*pstate).p_pre_columnref_hook = (*parentParseState).p_pre_columnref_hook;
        (*pstate).p_post_columnref_hook = (*parentParseState).p_post_columnref_hook;
        (*pstate).p_paramref_hook = (*parentParseState).p_paramref_hook;
        (*pstate).p_coerce_param_hook = (*parentParseState).p_coerce_param_hook;
        (*pstate).p_ref_hook_state = (*parentParseState).p_ref_hook_state;
        /* query environment stays in context for the whole parse analysis */
        (*pstate).p_queryEnv = (*parentParseState).p_queryEnv;
    }

    pstate
}

/*
 * free_parsestate
 *		Release a ParseState and any subsidiary resources.
 */
pub unsafe fn free_parsestate(pstate: *mut ParseState) {
    /*
     * Check that we did not produce too many resnos; at the very least we
     * cannot allow more than 2^16, since that would exceed the range of a
     * AttrNumber. It seems safest to use MaxTupleAttributeNumber.
     */
    if (*pstate).p_next_resno - 1 > MaxTupleAttributeNumber {
        let _ = errcode(ERRCODE_TOO_MANY_COLUMNS);
        ereport!(
            ERROR,
            errmsg!(
                "target lists can have at most {} entries",
                MaxTupleAttributeNumber
            )
        );
    }

    if !(*pstate).p_target_relation.is_null() {
        table_close((*pstate).p_target_relation, NoLock);
    }

    pfree(pstate as *mut c_void);
}

/*
 * parser_errposition
 *		Report a parse-analysis-time cursor position, if possible.
 *
 * This is expected to be used within an ereport() call.  The return value
 * is a dummy (always 0, in fact).
 */
#[no_mangle]
pub unsafe fn parser_errposition(pstate: *mut ParseState, location: c_int) -> c_int {
    let pos: c_int;

    /* No-op if location was not provided */
    if location < 0 {
        return 0;
    }
    /* Can't do anything if source text is not available */
    if pstate.is_null() || (*pstate).p_sourcetext.is_null() {
        return 0;
    }
    /* Convert offset to character number */
    pos = pg_mbstrlen_with_len((*pstate).p_sourcetext, location) + 1;
    /* And pass it to the ereport mechanism (mutates the in-flight ErrorData) */
    errposition(pos)
}

/*
 * setup_parser_errposition_callback
 *		Arrange for non-parser errors to report an error position
 */
pub unsafe fn setup_parser_errposition_callback(
    pcbstate: *mut ParseCallbackState,
    pstate: *mut ParseState,
    location: c_int,
) {
    /* Setup error traceback support for ereport() */
    (*pcbstate).pstate = pstate;
    (*pcbstate).location = location;
    (*pcbstate).errcallback.callback = Some(pcb_error_callback);
    (*pcbstate).errcallback.arg = pcbstate as *mut c_void;
    (*pcbstate).errcallback.previous = error_context_stack;
    error_context_stack = &mut (*pcbstate).errcallback;
}

/*
 * Cancel a previously-set-up errposition callback.
 */
pub unsafe fn cancel_parser_errposition_callback(pcbstate: *mut ParseCallbackState) {
    /* Pop the error context stack */
    error_context_stack = (*pcbstate).errcallback.previous;
}

/*
 * Error context callback for inserting parser error location.
 *
 * Note that this will be called for *any* error occurring while the
 * callback is installed.  We avoid inserting an irrelevant error location
 * if the error is a query cancel --- are there any other important cases?
 */
unsafe extern "C" fn pcb_error_callback(arg: *mut c_void) {
    let pcbstate: *mut ParseCallbackState = arg as *mut ParseCallbackState;

    if geterrcode() != ERRCODE_QUERY_CANCELED {
        let _ = parser_errposition((*pcbstate).pstate, (*pcbstate).location);
    }
}

/*
 * transformContainerType()
 *		Identify the actual container type for a subscripting operation.
 */
pub unsafe fn transformContainerType(containerType: *mut Oid, containerTypmod: *mut int32) {
    /*
     * If the input is a domain, smash to base type, and extract the actual
     * typmod to be applied to the base type.
     */
    *containerType = getBaseTypeAndTypmod(*containerType, containerTypmod);

    /*
     * We treat int2vector and oidvector as though they were domains over
     * int2[] and oid[].
     */
    if *containerType == INT2VECTOROID {
        *containerType = INT2ARRAYOID;
    } else if *containerType == OIDVECTOROID {
        *containerType = OIDARRAYOID;
    }
}

/*
 * transformContainerSubscripts()
 *		Transform container (array, etc) subscripting.  This is used for both
 *		container fetch and container assignment.
 */
pub unsafe fn transformContainerSubscripts(
    pstate: *mut ParseState,
    containerBase: *mut Node,
    mut containerType: Oid,
    mut containerTypMod: int32,
    indirection: *mut List,
    isAssignment: bool,
) -> *mut SubscriptingRef {
    let sbsref: *mut SubscriptingRef;
    let sbsroutines: *const SubscriptRoutines;
    let mut elementType: Oid = InvalidOid;
    let mut isSlice: bool = false;

    /*
     * Determine the actual container type, smashing any domain.  In the
     * assignment case the caller already did this.
     */
    if !isAssignment {
        transformContainerType(&mut containerType, &mut containerTypMod);
    }

    /*
     * Verify that the container type is subscriptable, and get its support
     * functions and typelem.
     */
    sbsroutines = getSubscriptingRoutines(containerType, &mut elementType);
    if sbsroutines.is_null() {
        let _ = errcode(ERRCODE_DATATYPE_MISMATCH);
        let _ = parser_errposition(pstate, exprLocation(containerBase));
        ereport!(
            ERROR,
            errmsg!(
                "cannot subscript type {:?} because it does not support subscripting",
                format_type_be(containerType)
            )
        );
    }

    /*
     * Detect whether any of the indirection items are slice specifiers.
     */
    foreach!(idx, indirection, {
        let ai: *mut A_Indices = lfirst_node!(A_Indices, T_A_Indices, current_cell!(idx));

        if (*ai).is_slice {
            isSlice = true;
            break;
        }
    });

    /*
     * Ready to build the SubscriptingRef node.
     */
    sbsref = makeNode!(SubscriptingRef, T_SubscriptingRef);

    (*sbsref).refcontainertype = containerType;
    (*sbsref).refelemtype = elementType;
    /* refrestype is to be set by container-specific logic */
    (*sbsref).reftypmod = containerTypMod;
    /* refcollid will be set by parse_collate.c */
    /* refupperindexpr, reflowerindexpr are to be set by container logic */
    (*sbsref).refexpr = containerBase as *mut Expr;
    (*sbsref).refassgnexpr = null_mut(); /* caller will fill if it's an assignment */

    /*
     * Call the container-type-specific logic to transform the subscripts and
     * determine the subscripting result type.
     */
    ((*sbsroutines).transform.unwrap())(
        sbsref,
        indirection,
        pstate as *mut crate::nodes::subscripting::ParseState,
        isSlice,
        isAssignment,
    );

    /*
     * Verify we got a valid type.
     */
    if !OidIsValid((*sbsref).refrestype) {
        let _ = errcode(ERRCODE_DATATYPE_MISMATCH);
        ereport!(
            ERROR,
            errmsg!(
                "cannot subscript type {:?} because it does not support subscripting",
                format_type_be(containerType)
            )
        );
    }

    sbsref
}

/*
 * make_const
 *
 *	Convert an A_Const node (as returned by the grammar) to a Const node
 *	of the "natural" type for the constant.
 */
pub unsafe fn make_const(pstate: *mut ParseState, aconst: *mut A_Const) -> *mut Const {
    let con: *mut Const;
    let val: Datum;
    let typeid: Oid;
    let typelen: c_int;
    let typebyval: bool;
    let mut pcbstate: ParseCallbackState = std::mem::zeroed();

    if (*aconst).isnull {
        /* return a null const */
        con = makeConst(
            UNKNOWNOID,
            -1,
            InvalidOid,
            -2,
            0 as Datum,
            true,
            false,
        );
        (*con).location = (*aconst).location;
        return con;
    }

    // &aconst->val is a pointer to the ValUnion, whose first member is a Node
    // (the value node's tag), so we can treat it as a *Node for nodeTag and the
    // intVal/boolVal/strVal accessors.
    let valnode: *mut Node = &mut (*aconst).val as *mut _ as *mut Node;

    match nodeTag(valnode) {
        NodeTag::T_Integer => {
            val = Int32GetDatum(intVal!(valnode));

            typeid = INT4OID;
            typelen = size_of::<int32>() as c_int;
            typebyval = true;
        }

        NodeTag::T_Float => {
            /* could be an oversize integer as well as a float ... */

            let mut escontext: ErrorSaveContext = std::mem::zeroed();
            escontext.r#type = NodeTag::T_ErrorSaveContext;
            let val64: int64;

            // NOTE: ManuallyDrop union field access (A_Const.val.fval.fval).
            // val.fval is ManuallyDrop<Float>, which derefs to Float.
            let fval: *mut c_char = (*(core::ptr::addr_of!((*aconst).val.fval) as *const crate::nodes::value::Float)).fval;
            val64 = pg_strtoint64_safe(fval, &mut escontext as *mut _ as *mut Node);
            if !escontext.error_occurred {
                /*
                 * It might actually fit in int32.  Probably only INT_MIN can
                 * occur, but we'll code the test generally just to be sure.
                 */
                let val32: int32 = val64 as int32;

                if val64 == val32 as int64 {
                    val = Int32GetDatum(val32);

                    typeid = INT4OID;
                    typelen = size_of::<int32>() as c_int;
                    typebyval = true;
                } else {
                    val = Int64GetDatum(val64);

                    typeid = INT8OID;
                    typelen = size_of::<int64>() as c_int;
                    typebyval = FLOAT8PASSBYVAL; /* int8 and float8 alike */
                }
            } else {
                /* arrange to report location if numeric_in() fails */
                setup_parser_errposition_callback(&mut pcbstate, pstate, (*aconst).location);
                val = DirectFunctionCall3!(
                    numeric_in,
                    CStringGetDatum(fval),
                    ObjectIdGetDatum(InvalidOid),
                    Int32GetDatum(-1)
                );
                cancel_parser_errposition_callback(&mut pcbstate);

                typeid = NUMERICOID;
                typelen = -1; /* variable len */
                typebyval = false;
            }
        }

        NodeTag::T_Boolean => {
            val = BoolGetDatum(boolVal!(valnode));

            typeid = BOOLOID;
            typelen = 1;
            typebyval = true;
        }

        NodeTag::T_String => {
            /*
             * We assume here that UNKNOWN's internal representation is the
             * same as CSTRING
             */
            val = CStringGetDatum(strVal!(valnode));

            typeid = UNKNOWNOID; /* will be coerced later */
            typelen = -2; /* cstring-style varwidth type */
            typebyval = false;
        }

        NodeTag::T_BitString => {
            /* arrange to report location if bit_in() fails */
            setup_parser_errposition_callback(&mut pcbstate, pstate, (*aconst).location);
            // NOTE: ManuallyDrop union field access (A_Const.val.bsval.bsval).
            // val.bsval is ManuallyDrop<BitString>, which derefs to BitString.
            let bsval: *mut c_char = (*(core::ptr::addr_of!((*aconst).val.bsval) as *const crate::nodes::value::BitString)).bsval;
            val = DirectFunctionCall3!(
                bit_in,
                CStringGetDatum(bsval),
                ObjectIdGetDatum(InvalidOid),
                Int32GetDatum(-1)
            );
            cancel_parser_errposition_callback(&mut pcbstate);
            typeid = BITOID;
            typelen = -1;
            typebyval = false;
        }

        _ => {
            elog!(ERROR, "unrecognized node type: {}", nodeTag(valnode) as c_int);
            unreachable!();
        }
    }

    con = makeConst(
        typeid,
        -1, /* typmod -1 is OK for all cases */
        InvalidOid, /* all cases are uncollatable types */
        typelen,
        val,
        false,
        typebyval,
    );
    (*con).location = (*aconst).location;

    con
}
