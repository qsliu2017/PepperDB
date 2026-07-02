//! Handle $n parameters in the parser. Translated from
//! backend/parser/parse_param.c (disposition: full leaf).
//!
//! Two cases used by the core backend:
//!  - a fixed list of parameters with known types (`setup_parse_fixed_parameters`,
//!    e.g. extended-protocol Parse with param OIDs, or `PREPARE p(t1,...)`);
//!  - an expandable list whose types are inferred from context
//!    (`setup_parse_variable_parameters`, e.g. an untyped `PREPARE`).
//!
//! Only explicit `$n` references (ParamRef nodes) are handled here; the core
//! parser knows nothing about Params and calls `pstate.p_paramref_hook`.
//!
//! In C the per-setup state (`FixedParamState` / `VarParamState`) is stashed in
//! `pstate->p_ref_hook_state` (a `void *`) and the hooks are plain C function
//! pointers. Rust `fn` pointers cannot capture, so the dispatch mechanism is
//! preserved verbatim - `p_paramref_hook`/`p_coerce_param_hook` stay `fn`
//! pointers - while the captured state moves into a typed `ParamRefHookState`
//! living in `pstate.p_ref_hook_state`. The hooks reach it through their
//! `&mut ParseState` argument (the analog of casting the `void *`).

use crate::backend::nodes::nodeFuncs::expression_tree_walker;
use crate::backend::utils::cache::lsyscache::get_typcollation;
use crate::catalog::genbki::{UNKNOWNOID, VOIDOID};
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::{ParamRef, Query};
use crate::nodes::primnodes::{Param, ParamKind};
use crate::parser::parse_node::{ParseExprKind, ParseState};
use crate::postgres_ext::{InvalidOid, Oid};

/// FixedParamState: the caller-supplied array of parameter type OIDs (C: an
/// immutable `const Oid *paramTypes` + `int numParams`).
pub struct FixedParamState {
    pub param_types: Vec<Oid>,
}

/// VarParamState: an expandable array of parameter type OIDs (the C
/// `Oid **paramTypes` re-`palloc`'d larger on demand). A zero entry means the
/// parameter number hasn't been seen yet; `UNKNOWNOID` means it's been used but
/// its type is not yet known. The growable `Vec<Oid>` subsumes both the C array
/// and its length counter.
pub struct VarParamState {
    pub param_types: Vec<Oid>,
}

/// The state behind the paramref/coerce hooks (C `void *p_ref_hook_state`).
/// `None` = no param setup; the two variants carry the fixed/variable arrays.
#[derive(Default)]
pub enum ParamRefHookState {
    #[default]
    None,
    Fixed(FixedParamState),
    Variable(VarParamState),
}

/// PG `setup_parse_fixed_parameters`: process a query referencing fixed params.
pub fn setup_parse_fixed_parameters(pstate: &mut ParseState, param_types: &[Oid]) {
    pstate.p_ref_hook_state = ParamRefHookState::Fixed(FixedParamState {
        param_types: param_types.to_vec(),
    });
    pstate.p_paramref_hook = Some(fixed_paramref_hook);
    // no need to use p_coerce_param_hook
}

/// PG `setup_parse_variable_parameters`: process a query referencing variable
/// params. `param_types` is in/out - the caller passes the (possibly empty)
/// known prefix and reads back the array the hooks grew.
pub fn setup_parse_variable_parameters(pstate: &mut ParseState, param_types: &[Oid]) {
    pstate.p_ref_hook_state = ParamRefHookState::Variable(VarParamState {
        param_types: param_types.to_vec(),
    });
    pstate.p_paramref_hook = Some(variable_paramref_hook);
    pstate.p_coerce_param_hook = Some(variable_coerce_param_hook);
}

/// Read back the resolved parameter type array after analysis (the analog of the
/// caller inspecting its `Oid **paramTypes` out-param for the variable case).
#[must_use]
pub fn collected_param_types(pstate: &ParseState) -> &[Oid] {
    match &pstate.p_ref_hook_state {
        ParamRefHookState::Fixed(s) => &s.param_types,
        ParamRefHookState::Variable(s) => &s.param_types,
        ParamRefHookState::None => &[],
    }
}

/// PG `fixed_paramref_hook`: transform a ParamRef using fixed parameter types.
#[allow(
    clippy::unnecessary_wraps,
    reason = "must match the ParseParamRefHook fn-pointer type (-> Option<Node>); a hook that returns None means transformParamRef raises 'no such parameter'"
)]
fn fixed_paramref_hook(pstate: &mut ParseState, pref: &mut ParamRef) -> Option<Node> {
    let ParamRefHookState::Fixed(parstate) = &pstate.p_ref_hook_state else {
        unreachable!("fixed_paramref_hook without FixedParamState");
    };
    let paramno = pref.number;
    let ok = paramno > 0
        && (paramno as usize) <= parstate.param_types.len()
        && crate::c::OidIsValid(parstate.param_types[paramno as usize - 1]);
    if !ok {
        no_such_parameter(paramno);
    }
    let paramtype = parstate.param_types[paramno as usize - 1];
    Some(Node::Param(Box::new(Param {
        paramkind: ParamKind::EXTERN,
        paramid: paramno,
        paramtype,
        paramtypmod: -1,
        paramcollid: get_typcollation(paramtype),
        location: pref.location,
    })))
}

/// PG `variable_paramref_hook`: transform a ParamRef using variable parameter
/// types, enlarging the type array as needed.
#[allow(
    clippy::unnecessary_wraps,
    reason = "must match the ParseParamRefHook fn-pointer type (-> Option<Node>); see fixed_paramref_hook"
)]
fn variable_paramref_hook(pstate: &mut ParseState, pref: &mut ParamRef) -> Option<Node> {
    let expr_kind = pstate.p_expr_kind;
    let ParamRefHookState::Variable(parstate) = &mut pstate.p_ref_hook_state else {
        unreachable!("variable_paramref_hook without VarParamState");
    };
    let paramno = pref.number;
    // Check parameter number is in range. (PG caps at MaxAllocSize/sizeof(Oid);
    // the practical cap here is "positive".)
    if paramno <= 0 {
        no_such_parameter(paramno);
    }
    // Enlarge the param array if needed (zero-filled, like repalloc0_array).
    if (paramno as usize) > parstate.param_types.len() {
        parstate.param_types.resize(paramno as usize, InvalidOid);
    }
    let slot = &mut parstate.param_types[paramno as usize - 1];
    // If not seen before, initialize to UNKNOWN type.
    if *slot == InvalidOid {
        *slot = UNKNOWNOID;
    }
    // void-in-CALL hack: a VOID argument in a procedure call is treated as unknown
    // (lets the JDBC driver avoid distinguishing function and procedure calls).
    if *slot == VOIDOID && expr_kind == ParseExprKind::CallArgument {
        *slot = UNKNOWNOID;
    }
    let paramtype = *slot;
    Some(Node::Param(Box::new(Param {
        paramkind: ParamKind::EXTERN,
        paramid: paramno,
        paramtype,
        paramtypmod: -1,
        paramcollid: get_typcollation(paramtype),
        location: pref.location,
    })))
}

/// PG `variable_coerce_param_hook`: coerce a Param to a query-requested datatype,
/// updating the inferred type when it was previously UNKNOWN. Returns `None` to
/// signal "proceed with normal coercion".
fn variable_coerce_param_hook(
    pstate: &mut ParseState,
    param: &mut Param,
    target_type_id: Oid,
    _target_type_mod: i32,
    location: i32,
) -> Option<Node> {
    if !(param.paramkind == ParamKind::EXTERN && param.paramtype == UNKNOWNOID) {
        // Else signal to proceed with normal coercion.
        return None;
    }
    let paramno = param.paramid;
    let ParamRefHookState::Variable(parstate) = &mut pstate.p_ref_hook_state else {
        unreachable!("variable_coerce_param_hook without VarParamState");
    };
    if paramno <= 0 || (paramno as usize) > parstate.param_types.len() {
        no_such_parameter(paramno);
    }
    let slot = &mut parstate.param_types[paramno as usize - 1];
    if *slot == UNKNOWNOID {
        // We've successfully resolved the type.
        *slot = target_type_id;
    } else if *slot == target_type_id {
        // We previously resolved the type, and it matches.
    } else {
        inconsistent_parameter(paramno);
    }
    param.paramtype = target_type_id;
    // Leave paramtypmod = -1 (run-time length check/coercion if needed).
    param.paramtypmod = -1;
    // This module always uses the type's default collation.
    param.paramcollid = get_typcollation(param.paramtype);
    // Use the leftmost of the param's and coercion's locations.
    if location >= 0 && (param.location < 0 || location < param.location) {
        param.location = location;
    }
    Some(Node::Param(Box::new(param.clone())))
}

/// PG `check_variable_parameters`: after analysis with variable parameters, verify
/// every PARAM_EXTERN symbol's type matches the resolved array (some Params may
/// still be UNKNOWN if nothing forced their coercion).
pub fn check_variable_parameters(pstate: &ParseState, query: &Query) {
    let ParamRefHookState::Variable(parstate) = &pstate.p_ref_hook_state else {
        unreachable!("check_variable_parameters without VarParamState");
    };
    // If numParams is zero then no Params were generated, so no work.
    if parstate.param_types.is_empty() {
        return;
    }
    walk_query_params(query, &mut |node| check_parameter_resolution_walker(node, &parstate.param_types));
}

/// PG `check_parameter_resolution_walker`: verify each PARAM_EXTERN Param against
/// the resolved type array. Returns `true` to abort the walk (PG's walker
/// convention; the abort is only used to short-circuit, never to signal "found").
fn check_parameter_resolution_walker(node: &Node, param_types: &[Oid]) -> bool {
    let Node::Param(param) = node else {
        return false;
    };
    if param.paramkind != ParamKind::EXTERN {
        return false;
    }
    let paramno = param.paramid;
    if paramno <= 0 || (paramno as usize) > param_types.len() {
        no_such_parameter(paramno);
    }
    if param.paramtype != param_types[paramno as usize - 1] {
        could_not_determine_type(paramno);
    }
    false
}

/// PG `query_contains_extern_params`: does a fully-parsed query tree contain any
/// PARAM_EXTERN Params?
#[must_use]
pub fn query_contains_extern_params(query: &Query) -> bool {
    let mut found = false;
    walk_query_params(query, &mut |node| {
        if let Node::Param(param) = node
            && param.paramkind == ParamKind::EXTERN
        {
            found = true;
            return true; // abort the walk
        }
        false
    });
    found
}

/// Visit `node` and every descendant expression node, calling `visit` on each
/// (depth-first, pre-order). Stops early if `visit` returns `true`. Built on the
/// backend `expression_tree_walker`, which recurses one node's sub-nodes; this
/// wrapper visits the node itself first, then re-descends through the walker.
fn visit_expr_tree(node: &Node, visit: &mut impl FnMut(&Node) -> bool) -> bool {
    if visit(node) {
        return true;
    }
    expression_tree_walker(node, |child| visit_expr_tree(child, visit))
}

/// Walk the expression-bearing fields of a `Query`, invoking `visit` on each node
/// (the analog of PG's `query_tree_walker` for the param-checking walkers, which
/// pass flag 0 - i.e. visit the targetlist, quals, and recurse into sublinks/RTE
/// subqueries). The deep RTE-subquery / CTE recursion grows when those Query
/// shapes become reachable; M9 reaches a flat SELECT targetlist + WHERE +
/// RETURNING.
fn walk_query_params(query: &Query, visit: &mut impl FnMut(&Node) -> bool) {
    for te in &query.targetList {
        if visit_expr_tree(te, visit) {
            return;
        }
    }
    if let Some(jt) = &query.jointree
        && visit_expr_tree(jt, visit)
    {
        return;
    }
    for rt in &query.returningList {
        if visit_expr_tree(rt, visit) {
            return;
        }
    }
}

#[cold]
fn no_such_parameter(paramno: i32) -> ! {
    crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_PARAMETER)
            .errmsg(format!("there is no parameter ${paramno}"));
    });
    unreachable!("ereport(ERROR) diverges");
}

#[cold]
fn inconsistent_parameter(paramno: i32) -> ! {
    crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(crate::utils::errcodes::ERRCODE_AMBIGUOUS_PARAMETER)
            .errmsg(format!("inconsistent types deduced for parameter ${paramno}"));
    });
    unreachable!("ereport(ERROR) diverges");
}

#[cold]
fn could_not_determine_type(paramno: i32) -> ! {
    crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(crate::utils::errcodes::ERRCODE_AMBIGUOUS_PARAMETER)
            .errmsg(format!("could not determine data type of parameter ${paramno}"));
    });
    unreachable!("ereport(ERROR) diverges");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::genbki::INT4OID;
    use crate::parser::parser::RawParseMode;

    /// Raw-parse `s` into its single RawStmt.
    fn raw(s: &str) -> crate::nodes::parsenodes::RawStmt {
        let mut list = crate::backend::parser::parser::raw_parser(s, RawParseMode::Default);
        assert_eq!(list.len(), 1);
        let Node::RawStmt(rs) = list.remove(0) else { panic!("not a RawStmt") };
        *rs
    }

    /// Pull the (only) target-list expression of an analyzed SELECT.
    fn first_target_expr(query: &Query) -> &Node {
        let Node::TargetEntry(te) = &query.targetList[0] else { panic!("not a TargetEntry") };
        te.expr.as_ref().expect("target has an expr")
    }

    #[test]
    fn fixed_param_resolves_to_declared_type() {
        // PG: `SELECT $1` analyzed with fixed param type [int4] -> $1 typed int4.
        let rs = raw("SELECT $1");
        let q = crate::backend::parser::analyze::parse_analyze_fixedparams(
            &rs,
            "SELECT $1",
            &[INT4OID],
            1,
            None,
        );
        let Node::Param(param) = first_target_expr(&q) else { panic!("not a Param") };
        assert_eq!(param.paramkind, ParamKind::EXTERN);
        assert_eq!(param.paramid, 1);
        assert_eq!(param.paramtype, INT4OID);
        assert_eq!(param.paramtypmod, -1);
    }

    #[test]
    fn variable_param_is_recorded_as_unknown() {
        // `SELECT $1` with variable params and no coercion to force the type ->
        // the param is recorded (as UNKNOWN) in the inferred type array.
        let rs = raw("SELECT $1");
        let mut param_types: Vec<Oid> = Vec::new();
        let q = crate::backend::parser::analyze::parse_analyze_varparams(
            &rs,
            "SELECT $1",
            &mut param_types,
            None,
        );
        // PG resolves a still-unknown output column to text
        // (resolveTargetListUnknowns -> variable_coerce_param_hook).
        assert_eq!(param_types.len(), 1);
        assert_eq!(param_types[0], crate::catalog::genbki::TEXTOID);
        let Node::Param(param) = first_target_expr(&q) else { panic!("not a Param") };
        assert_eq!(param.paramkind, ParamKind::EXTERN);
        assert_eq!(param.paramid, 1);
        assert_eq!(param.paramtype, crate::catalog::genbki::TEXTOID);
        assert!(query_contains_extern_params(&q));
    }
}
