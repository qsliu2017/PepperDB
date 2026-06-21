//! Translation of postgres/src/include/nodes/value.h
//!                + postgres/src/backend/nodes/value.c
//!
//! Interface and implementation for value nodes.
//!
//! The node types Integer, Float, String, and BitString are used to represent
//! literals in the lexer and are also used to pass constants around in the
//! parser.  One difference between these node types and, say, a plain int or
//! char * is that the nodes can be put into a List.
//!
//! (There used to be a Value node, which encompassed all these different node
//! types.  Hence the name of this file.)
//!
//! Copyright (c) 2003-2025, PostgreSQL Global Development Group
//!
//! ---------------------------------------------------------------------------
//! Translation notes (deviations from the C source):
//!
//! * The C accessor macros intVal/floatVal/boolVal/strVal are translated as
//!   `#[macro_export] macro_rules!`, each expanding to the same `castNode!` +
//!   field-access expression as the C `#define`.  They dereference raw pointers,
//!   so callers must invoke them inside `unsafe`.
//!
//! * `floatVal(v)` is `atof(castNode(Float, v)->fval)`; `atof` is the C library
//!   function (parses a NUL-terminated string to `double`), bound here via an
//!   `extern "C"` declaration to preserve the exact semantics.
//!
//! * The C `pg_node_attr(special_read_write)` marker is a no-op consumed by
//!   gen_node_support.pl; it has no runtime meaning and is omitted.

use crate::prelude::*;
use core::ffi::{c_char, c_int};

use crate::nodes::nodes::NodeTag;
use crate::makeNode;

// The C library `atof`, used by the `floatVal` accessor macro.
extern "C" {
    pub fn atof(nptr: *const c_char) -> f64;
}

/*
 * The node types Integer, Float, String, and BitString are used to represent
 * literals in the lexer and are also used to pass constants around in the
 * parser.  One difference between these node types and, say, a plain int or
 * char * is that the nodes can be put into a List.
 *
 * (There used to be a Value node, which encompassed all these different node
 * types.  Hence the name of this file.)
 */

#[repr(C)]
pub struct Integer {
    // pg_node_attr(special_read_write)
    pub r#type: NodeTag,
    pub ival: c_int,
}

/*
 * Float is internally represented as string.  Using T_Float as the node type
 * simply indicates that the contents of the string look like a valid numeric
 * literal.  The value might end up being converted to NUMERIC, so we can't
 * store it internally as a C double, since that could lose precision.  Since
 * these nodes are generally only used in the parsing process, not for runtime
 * data, it's better to use the more general representation.
 *
 * Note that an integer-looking string will get lexed as T_Float if the value
 * is too large to fit in an 'int'.
 */
#[repr(C)]
pub struct Float {
    // pg_node_attr(special_read_write)
    pub r#type: NodeTag,
    pub fval: *mut c_char,
}

#[repr(C)]
pub struct Boolean {
    // pg_node_attr(special_read_write)
    pub r#type: NodeTag,
    pub boolval: bool,
}

#[repr(C)]
pub struct String {
    // pg_node_attr(special_read_write)
    pub r#type: NodeTag,
    pub sval: *mut c_char,
}

#[repr(C)]
pub struct BitString {
    // pg_node_attr(special_read_write)
    pub r#type: NodeTag,
    pub bsval: *mut c_char,
}

/// `intVal(v)` -> `(castNode(Integer, v)->ival)`.
///
/// Dereferences a raw pointer; invoke inside `unsafe`.
#[macro_export]
macro_rules! intVal {
    ($v:expr) => {
        (*$crate::castNode!($crate::nodes::value::Integer, T_Integer, $v)).ival
    };
}

/// `floatVal(v)` -> `atof(castNode(Float, v)->fval)`.
///
/// Dereferences a raw pointer and calls the C library `atof`; invoke inside
/// `unsafe`.
#[macro_export]
macro_rules! floatVal {
    ($v:expr) => {
        $crate::nodes::value::atof((*$crate::castNode!($crate::nodes::value::Float, T_Float, $v)).fval)
    };
}

/// `boolVal(v)` -> `(castNode(Boolean, v)->boolval)`.
///
/// Dereferences a raw pointer; invoke inside `unsafe`.
#[macro_export]
macro_rules! boolVal {
    ($v:expr) => {
        (*$crate::castNode!($crate::nodes::value::Boolean, T_Boolean, $v)).boolval
    };
}

/// `strVal(v)` -> `(castNode(String, v)->sval)`.
///
/// Dereferences a raw pointer; invoke inside `unsafe`.
#[macro_export]
macro_rules! strVal {
    ($v:expr) => {
        (*$crate::castNode!($crate::nodes::value::String, T_String, $v)).sval
    };
}

/*
 *	makeInteger
 */
pub unsafe fn makeInteger(i: c_int) -> *mut Integer {
    let v: *mut Integer = makeNode!(Integer, T_Integer);

    (*v).ival = i;
    v
}

/*
 *	makeFloat
 *
 * Caller is responsible for passing a palloc'd string.
 */
pub unsafe fn makeFloat(numericStr: *mut c_char) -> *mut Float {
    let v: *mut Float = makeNode!(Float, T_Float);

    (*v).fval = numericStr;
    v
}

/*
 *	makeBoolean
 */
pub unsafe fn makeBoolean(val: bool) -> *mut Boolean {
    let v: *mut Boolean = makeNode!(Boolean, T_Boolean);

    (*v).boolval = val;
    v
}

/*
 *	makeString
 *
 * Caller is responsible for passing a palloc'd string.
 */
pub unsafe fn makeString(str: *mut c_char) -> *mut String {
    let v: *mut String = makeNode!(String, T_String);

    (*v).sval = str;
    v
}

/*
 *	makeBitString
 *
 * Caller is responsible for passing a palloc'd string.
 */
pub unsafe fn makeBitString(str: *mut c_char) -> *mut BitString {
    let v: *mut BitString = makeNode!(BitString, T_BitString);

    (*v).bsval = str;
    v
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{boolVal, intVal, strVal, IsA};

    #[test]
    fn make_and_read_value_nodes() {
        unsafe {
            let i = makeInteger(42);
            assert!(IsA!(i, T_Integer));
            assert_eq!(intVal!(i), 42);

            let b = makeBoolean(true);
            assert!(IsA!(b, T_Boolean));
            assert!(boolVal!(b));

            let s = makeString(c"hi".as_ptr() as *mut c_char);
            assert!(IsA!(s, T_String));
            let sv = strVal!(s);
            assert_eq!(*sv.add(0), b'h' as c_char);
            assert_eq!(*sv.add(1), b'i' as c_char);
        }
    }
}
