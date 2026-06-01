//! fe_utils/conditional.h - A stack of automaton states to handle nested conditionals.
//!
//! It is used by:
//! - "psql" interpreter for handling \if ... \endif
//! - "pgbench" interpreter for handling \if ... \endif
//! - "pgbench" syntax checker to test for proper nesting

use std::ffi::c_int;

/*
 * Possible states of a single level of \if block.
 */
pub type ifState = c_int;
pub const IFSTATE_NONE: ifState = 0; /* not currently in an \if block */
/* currently in an \if or \elif that is true and all parent branches (if any)
 * are true */
pub const IFSTATE_TRUE: ifState = 1;
/* currently in an \if or \elif that is false but no true branch has yet been
 * seen, and all parent branches (if any) are true */
pub const IFSTATE_FALSE: ifState = 2;
/* currently in an \elif that follows a true branch, or the whole \if is a
 * child of a false parent branch */
pub const IFSTATE_IGNORED: ifState = 3;
/* currently in an \else that is true and all parent branches (if any) are
 * true */
pub const IFSTATE_ELSE_TRUE: ifState = 4;
/* currently in an \else that is false or ignored */
pub const IFSTATE_ELSE_FALSE: ifState = 5;

/*
 * The state of nested \ifs is stored in a stack.
 *
 * query_len is used to determine what accumulated text to throw away at the
 * end of an inactive branch.  We also need to save and restore the lexer's
 * parenthesis nesting depth when throwing away text.
 */
#[repr(C)]
pub struct IfStackElem {
    pub if_state: ifState,            /* current state, see enum above */
    pub query_len: c_int,             /* length of query_buf at last branch start */
    pub paren_depth: c_int,           /* parenthesis depth at last branch start */
    pub next: *mut IfStackElem,       /* next surrounding \if, if any */
}

#[repr(C)]
pub struct ConditionalStackData {
    pub head: *mut IfStackElem,
}

pub type ConditionalStack = *mut ConditionalStackData;

pub unsafe fn conditional_stack_create() -> ConditionalStack {
    unimplemented!()
}

pub unsafe fn conditional_stack_reset(cstack: ConditionalStack) {
    unimplemented!()
}

pub unsafe fn conditional_stack_destroy(cstack: ConditionalStack) {
    unimplemented!()
}

pub unsafe fn conditional_stack_depth(cstack: ConditionalStack) -> c_int {
    unimplemented!()
}

pub unsafe fn conditional_stack_push(cstack: ConditionalStack, new_state: ifState) {
    unimplemented!()
}

pub unsafe fn conditional_stack_pop(cstack: ConditionalStack) -> bool {
    unimplemented!()
}

pub unsafe fn conditional_stack_peek(cstack: ConditionalStack) -> ifState {
    unimplemented!()
}

pub unsafe fn conditional_stack_poke(cstack: ConditionalStack, new_state: ifState) -> bool {
    unimplemented!()
}

pub unsafe fn conditional_stack_empty(cstack: ConditionalStack) -> bool {
    unimplemented!()
}

pub unsafe fn conditional_active(cstack: ConditionalStack) -> bool {
    unimplemented!()
}

pub unsafe fn conditional_stack_set_query_len(cstack: ConditionalStack, len: c_int) {
    unimplemented!()
}

pub unsafe fn conditional_stack_get_query_len(cstack: ConditionalStack) -> c_int {
    unimplemented!()
}

pub unsafe fn conditional_stack_set_paren_depth(cstack: ConditionalStack, depth: c_int) {
    unimplemented!()
}

pub unsafe fn conditional_stack_get_paren_depth(cstack: ConditionalStack) -> c_int {
    unimplemented!()
}
