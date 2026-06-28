//! Support routines for various kinds of object creation. Translated from
//! `src/backend/commands/define.c` (disposition: full).
//!
//! The `defGet*` family extracts a typed value from a `DefElem` (a generic
//! `name = value` option). They are the accessors `CREATE`/`ALTER` command code
//! uses to read option lists. Non-type-centric free functions; bodies here as
//! snake_case `pub fn`s, re-exported from the `defrem.h` header
//! (`crate::commands::defrem`) under the C names.
//!
//! Value-node arms (`T_Integer`/`T_Float`/`T_String`/`T_Boolean`/`T_List`) read a
//! `DefElem.arg` whose contents are PG value nodes; those are not yet `Node` enum
//! variants (`crate::nodes::value`; survey item B12), so the arms that would read
//! them route to a single staged guard until the node-defining pass adds the
//! variants. No M2 path builds a `DefElem` (a plain `CREATE TABLE` has no
//! options), so none of these is reached yet.

use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::{DefElem, TypeName};
use crate::postgres_ext::Oid;

/// A `DefElem.arg` carries a PG value node not yet representable as a `Node` enum
/// variant; staged per rules.md s4 until those variants land.
#[cold]
fn value_node_not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: DefElem value-node arg not yet a Node variant (survey B12)");
}

/// The DefElem name, for error messages (PG `def->defname`).
fn defname(def: &DefElem) -> &str {
    def.defname.as_deref().unwrap_or("")
}

/// PG `def->arg == NULL` "%s requires a parameter" error.
#[cold]
fn requires_parameter(def: &DefElem) -> ! {
    crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(crate::utils::errcodes::ERRCODE_SYNTAX_ERROR)
            .errmsg(format!("{} requires a parameter", defname(def)));
    });
    unreachable!("ereport(ERROR) diverges");
}

/// PG `def->arg` wrong-type error, with the per-accessor message.
#[cold]
fn requires(def: &DefElem, what: &str) -> ! {
    crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(crate::utils::errcodes::ERRCODE_SYNTAX_ERROR)
            .errmsg(format!("{} requires {what}", defname(def)));
    });
    unreachable!("ereport(ERROR) diverges");
}

/// PG `defGetString`: extract a string value from a DefElem.
pub fn defGetString(def: &DefElem) -> String {
    let Some(arg) = def.arg.as_ref() else { requires_parameter(def) };
    match arg {
        // PG `T_TypeName -> TypeNameToString`; that printer lives in parse_type.c
        // (header stub, not yet translated) -> staged with the value-node arms.
        Node::TypeName(_) => value_node_not_yet_reachable("defGetString: TypeName"),
        Node::A_Star(_) => "*".to_owned(),
        // T_Integer/T_Float/T_Boolean/T_String/T_List value-node arms (B12).
        _ => value_node_not_yet_reachable("defGetString"),
    }
}

/// PG `defGetBoolean`: extract a boolean value from a DefElem. A missing value
/// means "true".
pub fn defGetBoolean(def: &DefElem) -> bool {
    if def.arg.is_none() {
        return true;
    }
    // The T_Integer 0/1 fast path and the string-compare path both read value
    // nodes; staged with the rest of the value-node arms (B12).
    value_node_not_yet_reachable("defGetBoolean")
}

/// PG `defGetNumeric`: extract a numeric (f64) value from a DefElem.
pub fn defGetNumeric(def: &DefElem) -> f64 {
    if def.arg.is_none() {
        requires(def, "a numeric value");
    }
    value_node_not_yet_reachable("defGetNumeric")
}

/// PG `defGetInt32`: extract an int32 value from a DefElem.
pub fn defGetInt32(def: &DefElem) -> i32 {
    if def.arg.is_none() {
        requires(def, "an integer value");
    }
    value_node_not_yet_reachable("defGetInt32")
}

/// PG `defGetInt64`: extract an int64 value from a DefElem.
pub fn defGetInt64(def: &DefElem) -> i64 {
    if def.arg.is_none() {
        requires(def, "a numeric value");
    }
    value_node_not_yet_reachable("defGetInt64")
}

/// PG `defGetObjectId`: extract an OID value from a DefElem.
pub fn defGetObjectId(def: &DefElem) -> Oid {
    if def.arg.is_none() {
        requires(def, "a numeric value");
    }
    value_node_not_yet_reachable("defGetObjectId")
}

/// PG `defGetQualifiedName`: extract a possibly-qualified name (a list of String)
/// from a DefElem.
pub fn defGetQualifiedName(def: &DefElem) -> Vec<Node> {
    let Some(arg) = def.arg.as_ref() else { requires_parameter(def) };
    match arg {
        // TypeName.names is now Vec<String_> (narrowed); the qualified-name return
        // type is still the header's Vec<Node>, so this arm stages until the
        // qualified-name family is unified (survey B-family).
        Node::TypeName(_) => value_node_not_yet_reachable("defGetQualifiedName: TypeName.names"),
        _ => value_node_not_yet_reachable("defGetQualifiedName"),
    }
}

/// PG `defGetTypeName`: extract a TypeName from a DefElem.
pub fn defGetTypeName(def: &DefElem) -> TypeName {
    let Some(arg) = def.arg.as_ref() else { requires_parameter(def) };
    match arg {
        Node::TypeName(t) => (**t).clone(),
        // The T_String "compatibility" arm builds a TypeName from a string value
        // node (B12).
        _ => value_node_not_yet_reachable("defGetTypeName"),
    }
}

/// PG `defGetTypeLength`: extract a type length (int) from a DefElem.
pub fn defGetTypeLength(def: &DefElem) -> i32 {
    let Some(_arg) = def.arg.as_ref() else { requires_parameter(def) };
    value_node_not_yet_reachable("defGetTypeLength")
}

/// PG `defGetStringList`: extract a list of String values from a DefElem.
pub fn defGetStringList(def: &DefElem) -> Vec<Node> {
    let Some(_arg) = def.arg.as_ref() else { requires_parameter(def) };
    value_node_not_yet_reachable("defGetStringList")
}
