//! RestrictInfo node manipulation. Translated from
//! backend/optimizer/util/restrictinfo.c (disposition: leaf for the M3
//! single-relation scan qual; the OR-clause / join-clause analysis grows later).
//!
//! Non-type-centric free functions; bodies here, re-exported from
//! `crate::optimizer::restrictinfo` (the C declarations live in restrictinfo.h)
//! under the C names.
//!
//! A `RestrictInfo` wraps one AND sub-clause of a WHERE/JOIN-ON qual with the
//! bookkeeping the planner/costing/clause-extraction need. For M3 the only
//! reachable case is a base-relation restriction clause (a comparison OpExpr or a
//! BoolExpr over them) for a single-rel SeqScan: `make_restrictinfo` wraps it,
//! `extract_actual_clauses` unwraps the list back into bare clauses for the plan
//! node's `qual`. The relids analysis (`pull_varnos`, left/right-relid join-clause
//! detection) is deferred -- a single-rel scan qual references exactly the one
//! base rel and is never a join clause -- so the relid sets are left `None`
//! (rules.md s4: clean grow guards, not half-written logic).

#![allow(
    clippy::fn_params_excessive_bools,
    reason = "1:1 PG port: bool flags mirror the make_restrictinfo C signature"
)]
#![allow(
    clippy::boxed_local,
    reason = "1:1 PG port: clause/orclause are pointer-passed Expr nodes in the C signature"
)]

use crate::nodes::nodes::Node;
use crate::nodes::pathnodes::{
    PlannerInfo, QualCost, Relids, RestrictInfo, VolatileFunctionStatus,
};
use crate::nodes::primnodes::{Expr, Index};
use crate::postgres_ext::{InvalidOid, Oid};

/// Panic for a restrictinfo path not yet translated for this milestone
/// (rules.md s4).
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

/// Is this clause a top-level OR clause (`BoolExpr` with `OR_EXPR`)?
fn is_orclause(clause: &Node) -> bool {
    matches!(clause, Node::BoolExpr(b) if b.boolop == crate::nodes::primnodes::BoolExprType::OR_EXPR)
}

/// Is this clause a top-level AND clause?
fn is_andclause(clause: &Node) -> bool {
    matches!(clause, Node::BoolExpr(b) if b.boolop == crate::nodes::primnodes::BoolExprType::AND_EXPR)
}

/// PG `make_restrictinfo`: build a RestrictInfo for a WHERE/JOIN qual clause.
#[allow(clippy::too_many_arguments, reason = "1:1 PG port: matches the C signature")]
pub fn make_restrictinfo(
    root: &mut PlannerInfo,
    clause: Box<Expr>,
    is_pushed_down: bool,
    has_clone: bool,
    is_clone: bool,
    pseudoconstant: bool,
    security_level: Index,
    required_relids: Option<Relids>,
    incompatible_relids: Option<Relids>,
    outer_relids: Option<Relids>,
) -> RestrictInfo {
    if is_orclause(&clause) {
        not_yet_reachable("make_restrictinfo: OR clause (make_sub_restrictinfos)");
    }
    // AND/OR flattening should have removed top-level ANDs.
    crate::assert!(!is_andclause(&clause));

    make_plain_restrictinfo(
        root,
        clause,
        None,
        is_pushed_down,
        has_clone,
        is_clone,
        pseudoconstant,
        security_level,
        required_relids,
        incompatible_relids,
        outer_relids,
    )
}

/// PG `make_plain_restrictinfo`: construct a RestrictInfo from a non-OR clause.
/// The relid sets (left/right/clause) are computed by `pull_varnos` in PG; for
/// the single-rel M3 scan qual they are unused (no join-clause detection, no
/// index-path matching), so they are left `None` and grow with joins/indexes.
#[allow(clippy::too_many_arguments, reason = "1:1 PG port: matches the C signature")]
pub fn make_plain_restrictinfo(
    root: &mut PlannerInfo,
    clause: Box<Expr>,
    orclause: Option<Box<Expr>>,
    is_pushed_down: bool,
    has_clone: bool,
    is_clone: bool,
    pseudoconstant: bool,
    security_level: Index,
    required_relids: Option<Relids>,
    incompatible_relids: Option<Relids>,
    outer_relids: Option<Relids>,
) -> RestrictInfo {
    let _ = root;
    RestrictInfo {
        clause: *clause,
        orclause: orclause.map(|b| *b),
        is_pushed_down,
        pseudoconstant,
        has_clone,
        is_clone,
        can_join: false,
        security_level,
        incompatible_relids,
        outer_relids,
        // security_level == 0 quals are never delayed, so leakproof is "don't know".
        leakproof: false,
        has_volatile: VolatileFunctionStatus::UNKNOWN,
        // pull_varnos-derived relids are deferred (single-rel scan qual).
        clause_relids: None,
        left_relids: None,
        right_relids: None,
        required_relids,
        num_base_rels: 0,
        rinfo_serial: 0,
        parent_ec: None,
        eval_cost: QualCost { startup: -1.0, per_tuple: -1.0 },
        norm_selec: -1.0,
        outer_selec: -1.0,
        mergeopfamilies: Vec::new(),
        left_ec: None,
        right_ec: None,
        left_em: None,
        right_em: None,
        scansel_cache: Vec::new(),
        outer_is_left: false,
        hashjoinoperator: InvalidOid,
        left_bucketsize: -1.0,
        right_bucketsize: -1.0,
        left_mcvfreq: -1.0,
        right_mcvfreq: -1.0,
        left_hasheqoperator: InvalidOid,
        right_hasheqoperator: InvalidOid,
    }
}

/// PG `make_simple_restrictinfo` (restrictinfo.h convenience macro): a
/// valid-everywhere pushed-down qual at security level 0.
pub fn make_simple_restrictinfo(root: &mut PlannerInfo, clause: Box<Expr>) -> RestrictInfo {
    make_restrictinfo(root, clause, true, false, false, false, 0, None, None, None)
}

/// PG `get_actual_clauses`: strip the RestrictInfo wrappers, returning the bare
/// clauses. Asserts none are pseudoconstants (caller must use
/// `extract_actual_clauses` if pseudoconstants are possible).
pub fn get_actual_clauses(restrictinfo_list: &[RestrictInfo]) -> Vec<Node> {
    restrictinfo_list
        .iter()
        .map(|rinfo| {
            crate::assert!(!rinfo.pseudoconstant);
            rinfo.clause.clone()
        })
        .collect()
}

/// PG `extract_actual_clauses`: strip the RestrictInfo wrappers, keeping the
/// clauses whose `pseudoconstant` flag matches `pseudoconstant`. The scan-qual
/// caller passes `false` to get the per-tuple-evaluated clauses (pseudoconstants
/// become one-time gating quals handled elsewhere).
pub fn extract_actual_clauses(restrictinfo_list: &[RestrictInfo], pseudoconstant: bool) -> Vec<Node> {
    restrictinfo_list
        .iter()
        .filter(|rinfo| rinfo.pseudoconstant == pseudoconstant)
        .map(|rinfo| rinfo.clause.clone())
        .collect()
}

/// PG `commute_restrictinfo`: deferred (join-clause reordering, not reached at M3).
pub fn commute_restrictinfo(_rinfo: &RestrictInfo, _comm_op: Oid) -> RestrictInfo {
    not_yet_reachable("commute_restrictinfo")
}

/// PG `restriction_is_or_clause`.
pub fn restriction_is_or_clause(restrictinfo: &RestrictInfo) -> bool {
    restrictinfo.orclause.is_some()
}
