//! nodes/supportnodes.h - Definitions for planner support functions.
//!
//! A support function has SQL signature `supportfn(internal) returns internal`.
//! The argument is a pointer to one of the Node types defined here; the result
//! is usually also a Node pointer (NULL = "cannot help"). See the original
//! header for the full per-request semantics.

use std::ffi::c_int;

use crate::nodes::nodes::{Cost, JoinType, NodeTag, Selectivity};
use crate::nodes::pg_list::List;
use crate::nodes::primnodes::{FuncExpr, WindowFunc};
use crate::nodes::plannodes::MonotonicFunction;
use crate::postgres_ext::Oid;

// Forward-declared in the C header to avoid including pathnodes.h /
// parsenodes.h here.  We reference the real definitions where they live.
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::WindowClause;
use crate::nodes::pathnodes::{IndexOptInfo, PlannerInfo, SpecialJoinInfo};

/*
 * The Simplify request allows the support function to perform plan-time
 * simplification of a call to its target function.
 */
#[repr(C)]
pub struct SupportRequestSimplify {
    pub type_: NodeTag,

    pub root: *mut PlannerInfo, /* Planner's infrastructure */
    pub fcall: *mut FuncExpr,   /* Function call to be simplified */
}

/*
 * The Selectivity request allows the support function to provide a
 * selectivity estimate for a function appearing at top level of a WHERE
 * clause (so it applies only to functions returning boolean).
 */
#[repr(C)]
pub struct SupportRequestSelectivity {
    pub type_: NodeTag,

    /* Input fields: */
    pub root: *mut PlannerInfo,      /* Planner's infrastructure */
    pub funcid: Oid,                 /* function we are inquiring about */
    pub args: *mut List,             /* pre-simplified arguments to function */
    pub inputcollid: Oid,            /* function's input collation */
    pub is_join: bool,               /* is this a join or restriction case? */
    pub varRelid: c_int,             /* if restriction, RTI of target relation */
    pub jointype: JoinType,          /* if join, outer join type */
    pub sjinfo: *mut SpecialJoinInfo, /* if outer join, info about join */

    /* Output fields: */
    pub selectivity: Selectivity, /* returned selectivity estimate */
}

/*
 * The Cost request allows the support function to provide an execution
 * cost estimate for its target function.
 */
#[repr(C)]
pub struct SupportRequestCost {
    pub type_: NodeTag,

    /* Input fields: */
    pub root: *mut PlannerInfo, /* Planner's infrastructure (could be NULL) */
    pub funcid: Oid,            /* function we are inquiring about */
    pub node: *mut Node,        /* parse node invoking function, or NULL */

    /* Output fields: */
    pub startup: Cost,   /* one-time cost */
    pub per_tuple: Cost, /* per-evaluation cost */
}

/*
 * The Rows request allows the support function to provide an output rowcount
 * estimate for its target function (so it applies only to set-returning
 * functions).
 */
#[repr(C)]
pub struct SupportRequestRows {
    pub type_: NodeTag,

    /* Input fields: */
    pub root: *mut PlannerInfo, /* Planner's infrastructure (could be NULL) */
    pub funcid: Oid,            /* function we are inquiring about */
    pub node: *mut Node,        /* parse node invoking function */

    /* Output fields: */
    pub rows: f64, /* number of rows expected to be returned */
}

/*
 * The IndexCondition request allows the support function to generate
 * a directly-indexable condition based on a target function call that is
 * not itself indexable.
 */
#[repr(C)]
pub struct SupportRequestIndexCondition {
    pub type_: NodeTag,

    /* Input fields: */
    pub root: *mut PlannerInfo, /* Planner's infrastructure */
    pub funcid: Oid,            /* function we are inquiring about */
    pub node: *mut Node,        /* parse node invoking function */
    pub indexarg: c_int,        /* index of function arg matching indexcol */
    pub index: *mut IndexOptInfo, /* planner's info about target index */
    pub indexcol: c_int,        /* index of target index column (0-based) */
    pub opfamily: Oid,          /* index column's operator family */
    pub indexcollation: Oid,    /* index column's collation */

    /* Output fields: */
    pub lossy: bool, /* set to false if index condition is an exact
                      * equivalent of the function call */
}

/* ----------
 * To support more efficient query execution of any monotonically increasing
 * and/or monotonically decreasing window functions, we support calling the
 * window function's prosupport function passing along this struct whenever
 * the planner sees an OpExpr qual directly reference a window function in a
 * subquery.
 * ----------
 */
#[repr(C)]
pub struct SupportRequestWFuncMonotonic {
    pub type_: NodeTag,

    /* Input fields: */
    pub window_func: *mut WindowFunc,     /* Pointer to the window function data */
    pub window_clause: *mut WindowClause, /* Pointer to the window clause data */

    /* Output fields: */
    pub monotonic: MonotonicFunction,
}

/*
 * Some WindowFunc behavior might not be affected by certain variations in
 * the WindowClause's frameOptions.  Here we allow a WindowFunc's support
 * function to determine which, if anything, can be changed about the
 * WindowClause which the WindowFunc belongs to.
 */
#[repr(C)]
pub struct SupportRequestOptimizeWindowClause {
    pub type_: NodeTag,

    /* Input fields: */
    pub window_func: *mut WindowFunc,     /* Pointer to the window function data */
    pub window_clause: *mut WindowClause, /* Pointer to the window clause data */

    /* Input/Output fields: */
    pub frameOptions: c_int, /* New frameOptions, or left untouched if no
                              * optimizations are possible. */
}

/*
 * The ModifyInPlace request allows the support function to detect whether
 * a call to its target function can be allowed to modify a read/write
 * expanded object in-place.
 */
#[repr(C)]
pub struct SupportRequestModifyInPlace {
    pub type_: NodeTag,

    pub funcid: Oid,     /* PG_PROC OID of the target function */
    pub args: *mut List, /* Arguments to the function */
    pub paramid: c_int,  /* ID of Param(s) representing variable */
}
