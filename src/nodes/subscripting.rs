//! subscripting.h - API for generic type subscripting.

use std::ffi::c_void;

use crate::nodes::pg_list::List;
use crate::nodes::primnodes::SubscriptingRef;

// Forward declarations, to avoid including other headers.
// struct ParseState; struct SubscriptingRefState; struct SubscriptExecSteps;
// These are opaque here; referenced only via raw pointers in the method
// signatures below. Defined elsewhere (parser/SubscriptingRefState in
// executor/execExpr.h). Use opaque stubs to avoid blocking.
// TODO: dedup when parse_node.h / execExpr.h land.
pub type ParseState = c_void;
pub type SubscriptingRefState = c_void;
pub type SubscriptExecSteps = c_void;

/*
 * The transform method is called during parse analysis of a subscripting
 * construct.
 */
pub type SubscriptTransform = Option<
    unsafe extern "C" fn(
        sbsref: *mut SubscriptingRef,
        indirection: *mut List,
        pstate: *mut ParseState,
        isSlice: bool,
        isAssignment: bool,
    ),
>;

/*
 * The exec_setup method is called during executor-startup compilation of a
 * SubscriptingRef node in an expression.
 */
pub type SubscriptExecSetup = Option<
    unsafe extern "C" fn(
        sbsref: *const SubscriptingRef,
        sbsrefstate: *mut SubscriptingRefState,
        methods: *mut SubscriptExecSteps,
    ),
>;

/* Struct returned by the SQL-visible subscript handler function */
#[repr(C)]
pub struct SubscriptRoutines {
    pub transform: SubscriptTransform,       // parse analysis function
    pub exec_setup: SubscriptExecSetup,      // expression compilation function
    pub fetch_strict: bool,                  // is fetch SubscriptRef strict?
    pub fetch_leakproof: bool,               // is fetch SubscriptRef leakproof?
    pub store_leakproof: bool,               // is assignment SubscriptRef leakproof?
}
