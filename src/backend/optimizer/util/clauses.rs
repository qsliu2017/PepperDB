//! Expression-tree manipulation utilities. Translated from
//! backend/optimizer/util/clauses.c.
//!
//! Non-type-centric free functions; bodies here as snake_case `pub fn`s,
//! re-exported from `crate::optimizer::optimizer` (the C declarations live in
//! optimizer.h) under the C names.
//!
//! Disposition: `grow`. `eval_const_expressions` and its recursive driver
//! `eval_const_expressions_mutator` are the constant-folding entry point. M1's
//! live path folds a bare `Const` (a `Const` folds to itself). The mutator's
//! per-node-tag dispatch is scaffolded to grow: `OpExpr`/`FuncExpr`/`BoolExpr`
//! folding, `Param` substitution from boundParams, CASE simplification, and the
//! generic recurse-into-subexpressions default all route through a single
//! clearly-marked staging guard (rules.md s4); none is half-written. Later
//! milestones ADD arms (M4 operator/function folding, ...) rather than
//! restructure.

use crate::nodes::nodes::Node;
use crate::nodes::pathnodes::PlannerInfo;

/// Panic for a const-folding path not yet translated for this milestone
/// (rules.md s4).
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

/// Recursion context for `eval_const_expressions_mutator`.
///
/// PG threads an `eval_const_expressions_context` carrying boundParams, the
/// root, the active-functions stack (inlining recursion guard), the enclosing
/// CASE test value, and the `estimate` flag. For M1 only `estimate` is
/// meaningful (always false for `eval_const_expressions`); the other fields are
/// carried so the folding arms can grow into them. `boundParams` is not modeled
/// yet (no Params on the const path).
struct EvalConstContext<'a> {
    /// `for inlined-function dependencies` (PG `context.root`).
    #[allow(dead_code, reason = "grow: used by FuncExpr inlining / dependency recording (M4+)")]
    root: Option<&'a PlannerInfo>,
    /// safe transformations only (false) vs estimation (true).
    estimate: bool,
}

/// PG `eval_const_expressions`: perform constant simplification on the given
/// expression tree. `root` supplies boundParams for `PARAM_EXTERN` substitution
/// (None for a standalone expression).
pub fn eval_const_expressions(
    root: Option<&PlannerInfo>,
    node: Option<Node>,
) -> Option<Node> {
    // PG: context.boundParams = root ? root->glob->boundParams : NULL. boundParams
    // are not modeled yet; no Param appears on the M1 const path.
    let context = EvalConstContext { root, estimate: false };
    eval_const_expressions_mutator(node, &context)
}

/// PG `eval_const_expressions_mutator`: the recursive const-folding driver.
/// Dispatches on the node tag. M1 lives the `Const` (and NULL) cases; every
/// other tag routes to a grow guard.
fn eval_const_expressions_mutator(
    node: Option<Node>,
    context: &EvalConstContext<'_>,
) -> Option<Node> {
    let node = node?;

    match node {
        // A Const folds to itself, and a Var has no subexpressions (PG: the
        // generic default copies them unchanged).
        Node::Const(_) | Node::Var(_) => Some(node),

        // OpExpr/FuncExpr: recurse into the arguments. PG additionally tries
        // `simplify_function` -- evaluating the call when all args are Const and
        // the function is immutable (the `evaluate_expr` fold). That fold is
        // STAGED behind the volatility/permission metadata (deferred), so for now
        // the arguments are simplified in place and the call is left intact, which
        // is always safe (rules.md s4). The common case (an arg is a Var) is never
        // foldable anyway, exactly as PG would conclude.
        Node::OpExpr(mut op) => {
            op.args = simplify_args(std::mem::take(&mut op.args), context);
            Some(Node::OpExpr(op))
        }
        Node::FuncExpr(mut f) => {
            f.args = simplify_args(std::mem::take(&mut f.args), context);
            Some(Node::FuncExpr(f))
        }
        // BoolExpr: simplify the arguments. PG also folds out constant TRUE/FALSE
        // operands (e.g. `x AND true` -> `x`); that simplification is staged, so
        // the BoolExpr is rebuilt with simplified args.
        Node::BoolExpr(mut b) => {
            b.args = simplify_args(std::mem::take(&mut b.args), context);
            Some(Node::BoolExpr(b))
        }

        // Param substitution from boundParams (T_Param), DistinctExpr folding,
        // RelabelType/CoerceVia*, CASE/COALESCE/ArrayExpr simplification, and the
        // generic recurse-into-subexpressions default grow in later milestones.
        other => {
            let _ = context.estimate;
            not_yet_reachable(&format!("eval_const_expressions_mutator: {other:?}"));
        }
    }
}

/// Recurse `eval_const_expressions_mutator` over each argument of a call /
/// boolean node, dropping any that fold to nothing (none do at M3).
fn simplify_args(args: Vec<Node>, context: &EvalConstContext<'_>) -> Vec<Node> {
    args.into_iter()
        .filter_map(|a| eval_const_expressions_mutator(Some(a), context))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::genbki::INT4OID;
    use crate::nodes::primnodes::Const;
    use crate::postgres::{DatumGetInt32, Int32GetDatum};
    use crate::postgres_ext::InvalidOid;

    fn int4(v: i32) -> Node {
        Node::Const(Box::new(Const {
            consttype: INT4OID,
            consttypmod: -1,
            constcollid: InvalidOid,
            constlen: 4,
            constvalue: Int32GetDatum(v),
            constisnull: false,
            constbyval: true,
            location: -1,
        }))
    }

    #[test]
    fn const_folds_to_itself() {
        let folded = eval_const_expressions(None, Some(int4(5)));
        let Some(node) = folded else { panic!("a Const folds to a value, not None") };
        let Node::Const(c) = node else { panic!("a Const folds to a Const") };
        assert_eq!(c.consttype, INT4OID);
        assert_eq!(DatumGetInt32(c.constvalue), 5);
    }

    #[test]
    fn null_folds_to_null() {
        assert!(eval_const_expressions(None, None).is_none());
    }

    /// An OpExpr with a Var argument is not foldable; it is left intact (its args
    /// recursed but unchanged).
    #[test]
    fn opexpr_with_var_left_intact() {
        use crate::nodes::primnodes::{OpExpr, Var, VarReturningType};
        use crate::postgres_ext::Oid;
        let var = Node::Var(Box::new(Var {
            varno: 1,
            varattno: 1,
            vartype: INT4OID,
            vartypmod: -1,
            varcollid: InvalidOid,
            varnullingrels: None,
            varlevelsup: 0,
            varreturningtype: VarReturningType::DEFAULT,
            varnosyn: 1,
            varattnosyn: 1,
            location: -1,
        }));
        let op = Node::OpExpr(Box::new(OpExpr {
            opno: Oid(551),
            opfuncid: Oid(177),
            opresulttype: INT4OID,
            opretset: false,
            opcollid: InvalidOid,
            inputcollid: InvalidOid,
            args: vec![var, int4(1)],
            location: -1,
        }));
        let folded = eval_const_expressions(None, Some(op)).expect("OpExpr stays a node");
        let Node::OpExpr(o) = folded else { panic!("OpExpr must remain an OpExpr (not folded)") };
        assert_eq!(o.args.len(), 2);
        assert!(matches!(o.args[0], Node::Var(_)), "Var arg preserved");
        assert!(matches!(o.args[1], Node::Const(_)), "Const arg preserved");
    }
}
