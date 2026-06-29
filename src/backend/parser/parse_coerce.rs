//! Type coercion routines for the parser. Translated from
//! backend/parser/parse_coerce.c.
//!
//! Non-type-centric free functions (`coerce_to_target_type`, `coerce_type`, ...);
//! bodies here as snake_case `pub fn`s, re-exported from
//! `crate::parser::parse_coerce` under the C names.
//!
//! Disposition: `grow`. The coercion machinery (cast catalog lookups,
//! `numeric`/array/domain coercions, typmod coercion) reaches subsystems not
//! translated yet. The simple SELECT-constant path does not invoke coercion at
//! all (an int4 literal is already its final type and has no target type to
//! coerce to), so for M1 only the no-conversion identity arm is live; the general
//! dispatch routes to a single not-yet-reachable staging arm (rules.md s4).

use crate::nodes::nodes::Node;
use crate::nodes::primnodes::{CoercionContext, CoercionForm};
use crate::parser::parse_node::ParseState;
use crate::postgres_ext::Oid;

/// Panic for a coercion case that reaches the cast/typmod machinery not yet
/// translated for this milestone (rules.md s4).
#[cold]
fn not_yet_reachable() -> ! {
    unimplemented!("parse_coerce: coercion pathway not yet translated for this milestone");
}

/// PG `coerce_type`: convert an expression `node` of `input_type_id` to
/// `target_type_id`.
///
/// Only the leading no-conversion case is live for M1 (target == input, the only
/// case the SELECT-constant path could reach). Every actual coercion pathway
/// (polymorphic pseudotypes, UNKNOWN-const retyping, cast functions, binary
/// relabel, domain coercion) grows in later milestones.
pub fn coerce_type(
    _pstate: &mut ParseState,
    node: Node,
    input_type_id: Oid,
    target_type_id: Oid,
    _target_type_mod: i32,
    _ccontext: CoercionContext,
    _cformat: CoercionForm,
    _location: i32,
) -> Node {
    if target_type_id == input_type_id {
        // no conversion needed
        return node;
    }
    not_yet_reachable();
}

/// PG `coerce_to_target_type`: coerce `expr` to `targettype`/`targettypmod` in the
/// given context, or `None` if not coercible.
///
/// For M1, only the no-op identity (already the target type) is reachable; the
/// CollateExpr stripping, `can_coerce_type` cast search, and `coerce_type_typmod`
/// length coercion grow later.
pub fn coerce_to_target_type(
    pstate: &mut ParseState,
    expr: Option<Node>,
    exprtype: Oid,
    targettype: Oid,
    targettypmod: i32,
    ccontext: CoercionContext,
    cformat: CoercionForm,
    location: i32,
) -> Option<Node> {
    let expr = expr?;
    if exprtype == targettype {
        // Already the target type; no cast search or typmod coercion needed for
        // the M1-reachable case.
        return Some(coerce_type(
            pstate, expr, exprtype, targettype, targettypmod, ccontext, cformat, location,
        ));
    }
    not_yet_reachable();
}

/// PG `coerce_to_boolean`: coerce `expr` to type boolean (for WHERE/HAVING/etc).
/// M3 reaches the case where the expression is already boolean (a comparison
/// OpExpr) -- the identity. A non-boolean argument routes through `coerce_to_target_type`
/// (the implicit-cast search), which grows with the cast catalog; `coerce_to_boolean`
/// raises the "argument of X must be type boolean" error for an uncoercible type.
pub fn coerce_to_boolean(
    pstate: &mut ParseState,
    expr: Node,
    construct_name: &str,
) -> Node {
    use crate::catalog::genbki::BOOLOID;
    use crate::nodes::nodeFuncs::exprType;

    let input_type_id = exprType(&expr);
    if input_type_id == BOOLOID {
        return expr;
    }
    // Try an implicit coercion to bool (covers the unknown-literal case once casts
    // land). On failure, PG raises an error naming `construct_name`.
    let location = crate::nodes::nodeFuncs::exprLocation(&expr);
    coerce_to_target_type(
        pstate,
        Some(expr),
        input_type_id,
        BOOLOID,
        -1,
        CoercionContext::ASSIGNMENT,
        CoercionForm::IMPLICIT_CAST,
        location,
    )
    .unwrap_or_else(|| {
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_DATATYPE_MISMATCH)
                .errmsg(format!("argument of {construct_name} must be type boolean"));
        });
        unreachable!("ereport(ERROR) diverges");
    })
}
