//! Routines to compute clause selectivities. Translated from
//! backend/optimizer/path/clausesel.c (disposition: grow).
//!
//! Pre-M7 (real statistics) these return ROUGH default selectivities -- enough to
//! cost a qual'd scan and pick a plan, not real histogram/MCV estimates. PG routes
//! each clause through `clause_selectivity_ext` -> the operator's restriction
//! estimator (`eqsel`/`scalarltsel`/...), which consult pg_statistic. Until those
//! land, every clause gets a constant: `DEFAULT_EQ_SEL` for equality, the inequality
//! default for ordering comparisons, and a generic guess otherwise; a clause list's
//! selectivity is the product (independence assumption), which is exactly PG's
//! fallback when no stats exist.

#![allow(
    clippy::only_used_in_recursion,
    reason = "1:1 PG port: root/sjinfo match the clause_selectivity signature and feed the per-operator statistics estimators that grow at M7"
)]

use crate::nodes::nodes::{JoinType, Node, Selectivity};
use crate::nodes::pathnodes::{PlannerInfo, SpecialJoinInfo};

type PlannerInfoRef<'a> = &'a mut PlannerInfo;
type SpecialJoinInfoRef<'a> = &'a SpecialJoinInfo;

/// Generic default for a boolean clause with no better estimate.
const DEFAULT_CLAUSE_SEL: Selectivity = 0.333_333_333_333_333_3;

/// PG `clause_selectivity`: estimate the selectivity (fraction of rows passing) of
/// a single restriction clause.
pub fn clause_selectivity(
    root: PlannerInfoRef,
    clause: Option<Node>,
    var_relid: i32,
    jointype: JoinType,
    sjinfo: Option<SpecialJoinInfoRef>,
) -> Selectivity {
    clause_selectivity_ext(root, clause, var_relid, jointype, sjinfo, true)
}

/// PG `clause_selectivity_ext`: the rough-default estimator. `null` clause is
/// treated as TRUE (selectivity 1.0); a top-level AND multiplies its children, OR
/// uses inclusion-exclusion (approximated as the sum capped at 1.0), NOT inverts.
/// A comparison OpExpr gets the equality or inequality default by its operator
/// class; everything else gets the generic default.
pub fn clause_selectivity_ext(
    root: PlannerInfoRef,
    clause: Option<Node>,
    var_relid: i32,
    jointype: JoinType,
    sjinfo: Option<SpecialJoinInfoRef>,
    use_extended_stats: bool,
) -> Selectivity {
    let _ = (var_relid, jointype, use_extended_stats);
    let Some(clause) = clause else {
        return 1.0;
    };

    match &clause {
        // A RestrictInfo wraps the actual clause; recurse into it.
        Node::RestrictInfo(rinfo) => clause_selectivity_ext(
            root,
            Some(rinfo.clause.clone()),
            var_relid,
            jointype,
            sjinfo,
            use_extended_stats,
        ),
        Node::BoolExpr(b) => {
            use crate::nodes::primnodes::BoolExprType;
            match b.boolop {
                BoolExprType::AND_EXPR => b.args.iter().fold(1.0, |acc, a| {
                    acc * clause_selectivity_ext(
                        root,
                        Some(a.clone()),
                        var_relid,
                        jointype,
                        sjinfo,
                        use_extended_stats,
                    )
                }),
                BoolExprType::OR_EXPR => {
                    // s1 + s2 - s1*s2, iterated; clamp to [0,1].
                    let s = b.args.iter().fold(0.0_f64, |acc, a| {
                        let s = clause_selectivity_ext(
                            root,
                            Some(a.clone()),
                            var_relid,
                            jointype,
                            sjinfo,
                            use_extended_stats,
                        );
                        // s1 + s2 - s1*s2 (inclusion-exclusion), via mul_add.
                        (-acc).mul_add(s, acc + s)
                    });
                    s.clamp(0.0, 1.0)
                }
                BoolExprType::NOT_EXPR => {
                    let s = clause_selectivity_ext(
                        root,
                        b.args.first().cloned(),
                        var_relid,
                        jointype,
                        sjinfo,
                        use_extended_stats,
                    );
                    1.0 - s
                }
            }
        }
        // PG routes the OpExpr through the operator's selectivity estimator: a join
        // clause (both sides are Vars from different rels, var_relid == 0) uses the
        // `oprjoin` estimator, a restriction clause (var_relid != 0, or one side is a
        // pseudoconstant) uses the `oprrest` estimator. Both fall back to the no-stats
        // defaults in selfuncs (eqsel/eqjoinsel -> DEFAULT_EQ_SEL, scalar* ->
        // DEFAULT_INEQ_SEL) until ANALYZE lands.
        Node::OpExpr(op) => op_clause_selectivity(root, op, var_relid, jointype, sjinfo),
        // A bare boolean Var/Const, or any other clause: generic guess.
        _ => DEFAULT_CLAUSE_SEL,
    }
}

/// PG `clause_selectivity_ext`'s OpExpr arm: dispatch to the operator's restriction
/// or join selectivity estimator. A two-Var-side clause with `var_relid == 0` is a
/// join clause (use `oprjoin` -> selfuncs `join_selectivity`); otherwise it is a
/// restriction clause (use `oprrest` -> selfuncs `restriction_selectivity`). The
/// estimators read pg_statistic, which is absent (no ANALYZE), so they take the
/// no-stats default path.
fn op_clause_selectivity(
    root: PlannerInfoRef,
    op: &crate::nodes::primnodes::OpExpr,
    var_relid: i32,
    jointype: JoinType,
    sjinfo: Option<SpecialJoinInfoRef>,
) -> Selectivity {
    use crate::backend::utils::adt::selfuncs::{join_selectivity, restriction_selectivity};
    use crate::backend::utils::cache::lsyscache::{get_oprjoin, get_oprrest};

    let is_join_clause = var_relid == 0 && clause_is_join_clause(&op.args);
    if is_join_clause && let Some(sjinfo) = sjinfo {
        let oprjoin = get_oprjoin(op.opno);
        join_selectivity(root, oprjoin, op.opno, &op.args, jointype, sjinfo)
    } else {
        let oprrest = get_oprrest(op.opno);
        restriction_selectivity(root, oprrest, op.opno, &op.args, var_relid)
    }
}

/// Whether a binary operator clause is a join clause (a Var on each side, with the
/// two Vars from different relations). A restriction clause has a pseudoconstant on
/// one side.
fn clause_is_join_clause(args: &[Node]) -> bool {
    let [Node::Var(l), Node::Var(r)] = args else {
        return false;
    };
    l.varno != r.varno
}

/// PG `clauselist_selectivity`: combined selectivity of an implicit-AND clause
/// list (the product, under the independence assumption -- exactly PG's no-stats
/// fallback).
pub fn clauselist_selectivity(
    root: PlannerInfoRef,
    clauses: Vec<Node>,
    var_relid: i32,
    jointype: JoinType,
    sjinfo: Option<SpecialJoinInfoRef>,
) -> Selectivity {
    clauselist_selectivity_ext(root, clauses, var_relid, jointype, sjinfo, true)
}

/// PG `clauselist_selectivity_ext`: the rough-default list estimator.
pub fn clauselist_selectivity_ext(
    root: PlannerInfoRef,
    clauses: Vec<Node>,
    var_relid: i32,
    jointype: JoinType,
    sjinfo: Option<SpecialJoinInfoRef>,
    use_extended_stats: bool,
) -> Selectivity {
    clauses.into_iter().fold(1.0, |acc, c| {
        acc * clause_selectivity_ext(
            root,
            Some(c),
            var_relid,
            jointype,
            sjinfo,
            use_extended_stats,
        )
    })
}
