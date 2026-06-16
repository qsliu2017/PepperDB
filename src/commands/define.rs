//! Translation of postgres/src/backend/commands/define.c
//!                + the support-routine decls from
//!                  postgres/src/include/commands/defrem.h (define.c part only)
//!
//! Support routines for dealing with DefElem nodes (DDL option lists shared by
//! the CREATE/ALTER commands).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! ---------------------------------------------------------------------------
//! #include mapping:
//!   "postgres.h"                 -> crate::prelude::*
//!   <ctype.h>, <math.h>          -> (not needed; psprintf/atof handled inline)
//!   "catalog/namespace.h"        -> NameListToString (STUB)
//!   "commands/defrem.h"          -> this file's own decls
//!   "nodes/makefuncs.h"          -> crate::nodes::makefuncs::makeTypeNameFromNameList (REAL)
//!   "parser/parse_type.h"        -> TypeNameToString (STUB)
//!   "utils/fmgrprotos.h"         -> int8in / oidin / DirectFunctionCall1 (STUB)
//!
//! Translation notes (deviations from the C source):
//!   * C `psprintf("%ld", (long) intVal(...))` is rendered with Rust integer
//!     formatting into a palloc'd NUL-terminated C string via a small local
//!     helper `psprintf_long` (utils/mmgr/mcxt.c's psprintf is not yet ported).
//!   * C `pg_strcasecmp(sval, "...")` uses the ported port/pgstrcasecmp.rs.
//!   * `elog!/ereport!(ERROR, ...)` panic; the `return NULL/0/false/NIL`
//!     "keep compiler quiet" tails after them are unreachable and rendered with
//!     `unreachable!()` (or just a trailing value where control can fall
//!     through, as in defGetTypeLength).

use crate::prelude::*;

use crate::nodes::nodes::{Node, NodeTag};
// The intVal!/boolVal!/strVal! macros expand to castNode!(Integer/Boolean/String, ..),
// so those Value node types must be in scope by those exact names. `String` here
// shadows std String; std String uses below are fully-qualified.
use crate::nodes::value::{Boolean, Integer, String};
use crate::nodes::parsenodes::{DefElem, TypeName};
use crate::nodes::pg_list::{lfirst, List, NIL};
use crate::nodes::makefuncs::makeTypeNameFromNameList;
use crate::port::pgstrcasecmp::pg_strcasecmp;

use crate::{boolVal, castNode, current_cell, foreach, intVal, list_make1, strVal, IsA};

// TODO(pg-port): ERRCODE_SYNTAX_ERROR lives in the generated utils/errcodes.h,
// which is not yet ported.  The errcode() shim ignores its argument, so a
// placeholder value is harmless; replace with the real code on porting errcodes.
const ERRCODE_SYNTAX_ERROR: c_int = 0;

// C library strlen, bound directly (mirrors the pattern in common/relpath.rs).
extern "C" {
    fn strlen(s: *const c_char) -> usize;
}

// ----------------------------------------------------------------
// STUBbed dependencies (finest granularity).
// ----------------------------------------------------------------

/// STUB: parser/parse_type.c `TypeNameToString` -- renders a TypeName to its
/// textual form.  Not yet ported.
///
/// TODO: port parser/parse_type.c::TypeNameToString.
unsafe fn TypeNameToString(_typeName: *const TypeName) -> *mut c_char {
    unimplemented!("TypeNameToString: parser/parse_type.c not ported")
}

/// STUB: catalog/namespace.c `NameListToString` -- renders a List of String
/// nodes (a possibly-qualified name) to dotted text.  Not yet ported.
///
/// TODO: port catalog/namespace.c::NameListToString.
unsafe fn NameListToString(_names: *const List) -> *mut c_char {
    unimplemented!("NameListToString: catalog/namespace.c not ported")
}

/// STUB: utils/adt/int8.c `int8in` invoked via fmgr `DirectFunctionCall1` to
/// parse an int8 from a C string.  Used by defGetInt64 for Float-encoded large
/// integers.  Not yet ported.
///
/// TODO: port utils/adt/int8.c::int8in (+ fmgr DirectFunctionCall1).
unsafe fn directcall_int8in(_str: *const c_char) -> int64 {
    unimplemented!("int8in/DirectFunctionCall1: utils/adt/int8.c not ported")
}

/// STUB: utils/adt/oid.c `oidin` invoked via fmgr `DirectFunctionCall1` to parse
/// an Oid from a C string.  Used by defGetObjectId for Float-encoded large
/// values.  Not yet ported.
///
/// TODO: port utils/adt/oid.c::oidin (+ fmgr DirectFunctionCall1).
unsafe fn directcall_oidin(_str: *const c_char) -> Oid {
    unimplemented!("oidin/DirectFunctionCall1: utils/adt/oid.c not ported")
}

/// STUB: parser/parse_node.c `parser_errposition` -- attaches the source
/// position of a token to an in-flight error report.  Here the position is
/// already consumed by the panicking ereport!, so this is unreachable; kept as
/// a STUB for signature fidelity.
///
/// TODO: port parser/parse_node.c::parser_errposition.
unsafe fn parser_errposition(_pstate: *mut ParseState, _location: c_int) -> c_int {
    unimplemented!("parser_errposition: parser/parse_node.c not ported")
}

/// STUB: parser ParseState (parser/parse_node.h).  Only used as an opaque
/// pointer argument to errorConflictingDefElem here.
///
/// TODO: replace with the real ParseState once parser/parse_node.c is ported.
pub enum ParseState {}

// ----------------------------------------------------------------
// Local helper: psprintf("%ld", v) for the integer-to-string path.
// ----------------------------------------------------------------

/// Render a `c_long` to a palloc'd NUL-terminated C string (the C source uses
/// `psprintf("%ld", v)`).  utils/mmgr/mcxt.c::psprintf is not yet ported, so we
/// format with Rust and copy into palloc'd memory.
unsafe fn psprintf_long(v: c_long) -> *mut c_char {
    let s = format!("{}\0", v);
    let bytes = s.as_bytes();
    let p = palloc(bytes.len()) as *mut c_char;
    core::ptr::copy_nonoverlapping(bytes.as_ptr() as *const c_char, p, bytes.len());
    p
}

/*
 * Extract a string value (otherwise uninterpreted) from a DefElem.
 */
pub unsafe fn defGetString(def: *mut DefElem) -> *mut c_char {
    if (*def).arg.is_null() {
        ereport!(
            ERROR,
            errmsg!("{} requires a parameter", cstr_display((*def).defname))
        );
    }
    match crate::nodes::nodes::nodeTag((*def).arg) {
        NodeTag::T_Integer => psprintf_long(intVal!((*def).arg) as c_long),
        NodeTag::T_Float => (*castNode!(crate::nodes::value::Float, T_Float, (*def).arg)).fval,
        NodeTag::T_Boolean => {
            if boolVal!((*def).arg) {
                c"true".as_ptr() as *mut c_char
            } else {
                c"false".as_ptr() as *mut c_char
            }
        }
        NodeTag::T_String => strVal!((*def).arg),
        NodeTag::T_TypeName => TypeNameToString((*def).arg as *const TypeName),
        NodeTag::T_List => NameListToString((*def).arg as *const List),
        NodeTag::T_A_Star => pstrdup(c"*".as_ptr()),
        other => {
            elog!(ERROR, "unrecognized node type: {}", other as c_int);
            unreachable!()
        }
    }
}

/*
 * Extract a numeric value (actually double) from a DefElem.
 */
pub unsafe fn defGetNumeric(def: *mut DefElem) -> f64 {
    if (*def).arg.is_null() {
        ereport!(
            ERROR,
            errmsg!("{} requires a numeric value", cstr_display((*def).defname))
        );
    }
    match crate::nodes::nodes::nodeTag((*def).arg) {
        NodeTag::T_Integer => intVal!((*def).arg) as f64,
        NodeTag::T_Float => floatVal!((*def).arg),
        _ => {
            ereport!(
                ERROR,
                errmsg!("{} requires a numeric value", cstr_display((*def).defname))
            );
            unreachable!()
        }
    }
}

/*
 * Extract a boolean value from a DefElem.
 */
pub unsafe fn defGetBoolean(def: *mut DefElem) -> bool {
    /*
     * If no parameter value given, assume "true" is meant.
     */
    if (*def).arg.is_null() {
        return true;
    }

    /*
     * Allow 0, 1, "true", "false", "on", "off"
     */
    match crate::nodes::nodes::nodeTag((*def).arg) {
        NodeTag::T_Integer => match intVal!((*def).arg) {
            0 => return false,
            1 => return true,
            _ => { /* otherwise, error out below */ }
        },
        _ => {
            let sval = defGetString(def);

            /*
             * The set of strings accepted here should match up with the
             * grammar's opt_boolean_or_string production.
             */
            if pg_strcasecmp(sval, c"true".as_ptr()) == 0 {
                return true;
            }
            if pg_strcasecmp(sval, c"false".as_ptr()) == 0 {
                return false;
            }
            if pg_strcasecmp(sval, c"on".as_ptr()) == 0 {
                return true;
            }
            if pg_strcasecmp(sval, c"off".as_ptr()) == 0 {
                return false;
            }
        }
    }
    ereport!(
        ERROR,
        errmsg!("{} requires a Boolean value", cstr_display((*def).defname))
    );
    unreachable!()
}

/*
 * Extract an int32 value from a DefElem.
 */
pub unsafe fn defGetInt32(def: *mut DefElem) -> int32 {
    if (*def).arg.is_null() {
        ereport!(
            ERROR,
            errmsg!("{} requires an integer value", cstr_display((*def).defname))
        );
    }
    match crate::nodes::nodes::nodeTag((*def).arg) {
        NodeTag::T_Integer => intVal!((*def).arg) as int32,
        _ => {
            ereport!(
                ERROR,
                errmsg!("{} requires an integer value", cstr_display((*def).defname))
            );
            unreachable!()
        }
    }
}

/*
 * Extract an int64 value from a DefElem.
 */
pub unsafe fn defGetInt64(def: *mut DefElem) -> int64 {
    if (*def).arg.is_null() {
        ereport!(
            ERROR,
            errmsg!("{} requires a numeric value", cstr_display((*def).defname))
        );
    }
    match crate::nodes::nodes::nodeTag((*def).arg) {
        NodeTag::T_Integer => intVal!((*def).arg) as int64,
        NodeTag::T_Float => {
            /*
             * Values too large for int4 will be represented as Float
             * constants by the lexer.  Accept these if they are valid int8
             * strings.
             */
            directcall_int8in((*castNode!(crate::nodes::value::Float, T_Float, (*def).arg)).fval)
        }
        _ => {
            ereport!(
                ERROR,
                errmsg!("{} requires a numeric value", cstr_display((*def).defname))
            );
            unreachable!()
        }
    }
}

/*
 * Extract an OID value from a DefElem.
 */
pub unsafe fn defGetObjectId(def: *mut DefElem) -> Oid {
    if (*def).arg.is_null() {
        ereport!(
            ERROR,
            errmsg!("{} requires a numeric value", cstr_display((*def).defname))
        );
    }
    match crate::nodes::nodes::nodeTag((*def).arg) {
        NodeTag::T_Integer => intVal!((*def).arg) as Oid,
        NodeTag::T_Float => {
            /*
             * Values too large for int4 will be represented as Float
             * constants by the lexer.  Accept these if they are valid OID
             * strings.
             */
            directcall_oidin((*castNode!(crate::nodes::value::Float, T_Float, (*def).arg)).fval)
        }
        _ => {
            ereport!(
                ERROR,
                errmsg!("{} requires a numeric value", cstr_display((*def).defname))
            );
            unreachable!()
        }
    }
}

/*
 * Extract a possibly-qualified name (as a List of Strings) from a DefElem.
 */
pub unsafe fn defGetQualifiedName(def: *mut DefElem) -> *mut List {
    if (*def).arg.is_null() {
        ereport!(
            ERROR,
            errmsg!("{} requires a parameter", cstr_display((*def).defname))
        );
    }
    match crate::nodes::nodes::nodeTag((*def).arg) {
        NodeTag::T_TypeName => (*((*def).arg as *mut TypeName)).names,
        NodeTag::T_List => (*def).arg as *mut List,
        NodeTag::T_String => {
            /* Allow quoted name for backwards compatibility */
            list_make1!((*def).arg as *mut c_void)
        }
        _ => {
            ereport!(
                ERROR,
                errmsg!("argument of {} must be a name", cstr_display((*def).defname))
            );
            unreachable!()
        }
    }
}

/*
 * Extract a TypeName from a DefElem.
 *
 * Note: we do not accept a List arg here, because the parser will only
 * return a bare List when the name looks like an operator name.
 */
pub unsafe fn defGetTypeName(def: *mut DefElem) -> *mut TypeName {
    if (*def).arg.is_null() {
        ereport!(
            ERROR,
            errmsg!("{} requires a parameter", cstr_display((*def).defname))
        );
    }
    match crate::nodes::nodes::nodeTag((*def).arg) {
        NodeTag::T_TypeName => (*def).arg as *mut TypeName,
        NodeTag::T_String => {
            /* Allow quoted typename for backwards compatibility */
            makeTypeNameFromNameList(list_make1!((*def).arg as *mut c_void))
        }
        _ => {
            ereport!(
                ERROR,
                errmsg!("argument of {} must be a type name", cstr_display((*def).defname))
            );
            unreachable!()
        }
    }
}

/*
 * Extract a type length indicator (either absolute bytes, or
 * -1 for "variable") from a DefElem.
 */
pub unsafe fn defGetTypeLength(def: *mut DefElem) -> c_int {
    if (*def).arg.is_null() {
        ereport!(
            ERROR,
            errmsg!("{} requires a parameter", cstr_display((*def).defname))
        );
    }
    match crate::nodes::nodes::nodeTag((*def).arg) {
        NodeTag::T_Integer => return intVal!((*def).arg),
        NodeTag::T_Float => {
            ereport!(
                ERROR,
                errmsg!("{} requires an integer value", cstr_display((*def).defname))
            );
        }
        NodeTag::T_String => {
            if pg_strcasecmp(strVal!((*def).arg), c"variable".as_ptr()) == 0 {
                return -1; /* variable length */
            }
        }
        NodeTag::T_TypeName => {
            /* cope if grammar chooses to believe "variable" is a typename */
            if pg_strcasecmp(
                TypeNameToString((*def).arg as *const TypeName),
                c"variable".as_ptr(),
            ) == 0
            {
                return -1; /* variable length */
            }
        }
        NodeTag::T_List => { /* must be an operator name */ }
        other => {
            elog!(ERROR, "unrecognized node type: {}", other as c_int);
        }
    }
    ereport!(
        ERROR,
        errmsg!(
            "invalid argument for {}: \"{}\"",
            cstr_display((*def).defname),
            cstr_display(defGetString(def))
        )
    );
    unreachable!()
}

/*
 * Extract a list of string values (otherwise uninterpreted) from a DefElem.
 */
pub unsafe fn defGetStringList(def: *mut DefElem) -> *mut List {
    if (*def).arg.is_null() {
        ereport!(
            ERROR,
            errmsg!("{} requires a parameter", cstr_display((*def).defname))
        );
    }
    if !IsA!((*def).arg, T_List) {
        elog!(
            ERROR,
            "unrecognized node type: {}",
            crate::nodes::nodes::nodeTag((*def).arg) as c_int
        );
    }

    foreach!(cell, (*def).arg as *mut List, {
        let str = lfirst(current_cell!(cell)) as *mut Node;

        if !IsA!(str, T_String) {
            elog!(
                ERROR,
                "unexpected node type in name list: {}",
                crate::nodes::nodes::nodeTag(str) as c_int
            );
        }
    });

    (*def).arg as *mut List
}

/*
 * Raise an error about a conflicting DefElem.
 */
pub unsafe fn errorConflictingDefElem(defel: *mut DefElem, pstate: *mut ParseState) -> ! {
    // parser_errposition is consumed by the (panicking) ereport in C; the call
    // is unreachable here because ereport!(ERROR) never returns, so we name it
    // only to preserve the dependency surface.
    let _ = (defel, pstate);
    ereport!(
        ERROR,
        errmsg!("conflicting or redundant options")
    );
    unreachable!()
}

// ----------------------------------------------------------------
// Local display helper for C strings in errmsg! ("{}") formatting.
// ----------------------------------------------------------------

/// Wrap a `*const c_char` so it renders as a Rust `{}` string (lossy UTF-8).
/// Used because the C source interpolates `def->defname` etc. into errmsg().
fn cstr_display(s: *const c_char) -> std::string::String {
    if s.is_null() {
        return std::string::String::new();
    }
    unsafe {
        let len = strlen(s);
        let bytes = core::slice::from_raw_parts(s as *const u8, len);
        std::string::String::from_utf8_lossy(bytes).into_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::value::{makeBoolean, makeInteger, makeString};
    use crate::nodes::parsenodes::DEFELEM_UNSPEC;

    // Build a DefElem with the given arg Node, defname "opt".
    unsafe fn mk_def(arg: *mut Node) -> *mut DefElem {
        let d = palloc0(core::mem::size_of::<DefElem>()) as *mut DefElem;
        (*d).r#type = NodeTag::T_DefElem;
        (*d).defnamespace = null_mut();
        (*d).defname = c"opt".as_ptr() as *mut c_char;
        (*d).arg = arg;
        (*d).defaction = DEFELEM_UNSPEC;
        (*d).location = -1;
        d
    }

    #[test]
    fn boolean_from_string_and_integer() {
        unsafe {
            let t = mk_def(makeString(c"true".as_ptr() as *mut c_char) as *mut Node);
            assert!(defGetBoolean(t));

            let f = mk_def(makeString(c"off".as_ptr() as *mut c_char) as *mut Node);
            assert!(!defGetBoolean(f));

            let one = mk_def(makeInteger(1) as *mut Node);
            assert!(defGetBoolean(one));

            let zero = mk_def(makeInteger(0) as *mut Node);
            assert!(!defGetBoolean(zero));

            let b = mk_def(makeBoolean(true) as *mut Node);
            assert!(defGetBoolean(b));
        }
    }

    #[test]
    fn string_from_string_node() {
        unsafe {
            let d = mk_def(makeString(c"hello".as_ptr() as *mut c_char) as *mut Node);
            let s = defGetString(d);
            assert_eq!(strlen(s), 5);
            assert_eq!(*s.add(0), b'h' as c_char);
            assert_eq!(*s.add(4), b'o' as c_char);
        }
    }

    #[test]
    fn int32_from_integer_node() {
        unsafe {
            let d = mk_def(makeInteger(42) as *mut Node);
            assert_eq!(defGetInt32(d), 42);
            assert_eq!(defGetInt64(d), 42);
        }
    }
}
