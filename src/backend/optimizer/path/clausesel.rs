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

/// selfuncs.h `DEFAULT_EQ_SEL`: default selectivity for "A = B".
const DEFAULT_EQ_SEL: Selectivity = 0.005;
/// selfuncs.h `DEFAULT_INEQ_SEL`: default selectivity for "A < B", "A > B" etc.
const DEFAULT_INEQ_SEL: Selectivity = 0.333_333_333_333_333_3;
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
        Node::OpExpr(op) => op_clause_default_selectivity(op.opno),
        // A bare boolean Var/Const, or any other clause: generic guess.
        _ => DEFAULT_CLAUSE_SEL,
    }
}

/// Rough default for a comparison operator by OID: equality operators get
/// `DEFAULT_EQ_SEL`, ordering comparisons get `DEFAULT_INEQ_SEL`. Pre-statistics
/// we only need to distinguish "=" from the ordering comparisons; the exact
/// operator set grows with the type set.
///
/// This serves both restriction clauses ("a = const", PG `eqsel`) and join clauses
/// ("a.x = b.y", PG `eqjoinsel`): with no pg_statistic, both estimators fall back to
/// `DEFAULT_EQ_SEL`, which is what drives the join-rel row estimate in
/// `calc_joinrel_size_estimate` (costsize.rs). The per-operator estimators that read
/// real stats (eqsel/eqjoinsel/scalarltsel) are selfuncs, step 31.
fn op_clause_default_selectivity(opno: crate::postgres_ext::Oid) -> Selectivity {
    // pg_operator.dat "=" OIDs: bool(91), int2(94), int4(96), text(98), int8(410),
    // oid(607). (Cross-type "=" OIDs grow with the selfuncs lookup.)
    let is_equality = matches!(opno.0, 91 | 94 | 96 | 98 | 410 | 607);
    if is_equality {
        DEFAULT_EQ_SEL
    } else {
        DEFAULT_INEQ_SEL
    }
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
