//! RestrictInfo node manipulation. Translated from
//! backend/optimizer/util/restrictinfo.c. Covers the single-relation scan qual
//! and the OR-clause path (`make_sub_restrictinfos` wraps each OR/leaf subclause
//! in a RestrictInfo); the join-clause selectivity analysis grows later.
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

/// Is this opclause binary (exactly two args)? PG's
/// `list_length(((OpExpr *) clause)->args) == 2`.
fn opclause_is_binary(clause: &Node) -> bool {
    matches!(clause, Node::OpExpr(e) if e.args.len() == 2)
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
    // If it's an OR clause, build a modified copy with RestrictInfos inserted
    // above each subclause of the top-level AND/OR structure.
    if is_orclause(&clause) {
        let node = make_sub_restrictinfos(
            root,
            *clause,
            is_pushed_down,
            has_clone,
            is_clone,
            pseudoconstant,
            security_level,
            required_relids,
            incompatible_relids,
            outer_relids,
        );
        let Node::RestrictInfo(ri) = node else {
            unreachable!("make_restrictinfo: OR clause did not yield a RestrictInfo")
        };
        return *ri;
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

/// PG `make_sub_restrictinfos`: build a modified copy of an AND/OR clause tree
/// with RestrictInfos inserted above the OR subclauses (but not above sub-ANDs,
/// since only ORs and simple clauses are valid RestrictInfos). Returns either a
/// `Node::RestrictInfo` (OR / leaf) or a plain AND `BoolExpr` (AND).
#[allow(clippy::too_many_arguments, reason = "1:1 PG port: matches the C signature")]
fn make_sub_restrictinfos(
    root: &mut PlannerInfo,
    clause: Expr,
    is_pushed_down: bool,
    has_clone: bool,
    is_clone: bool,
    pseudoconstant: bool,
    security_level: Index,
    required_relids: Option<Relids>,
    incompatible_relids: Option<Relids>,
    outer_relids: Option<Relids>,
) -> Node {
    use crate::nodes::makefuncs::{make_andclause, make_orclause};
    if is_orclause(&clause) {
        let Node::BoolExpr(be) = &clause else { unreachable!() };
        let orlist: Vec<Node> = be
            .args
            .clone()
            .into_iter()
            .map(|arg| {
                make_sub_restrictinfos(
                    root,
                    arg,
                    is_pushed_down,
                    has_clone,
                    is_clone,
                    pseudoconstant,
                    security_level,
                    None, // OR constituents default to just their contained rels
                    incompatible_relids.clone(),
                    outer_relids.clone(),
                )
            })
            .collect();
        Node::RestrictInfo(Box::new(make_plain_restrictinfo(
            root,
            Box::new(clause),
            Some(Box::new(make_orclause(orlist))),
            is_pushed_down,
            has_clone,
            is_clone,
            pseudoconstant,
            security_level,
            required_relids,
            incompatible_relids,
            outer_relids,
        )))
    } else if is_andclause(&clause) {
        let Node::BoolExpr(be) = &clause else { unreachable!() };
        let andlist: Vec<Node> = be
            .args
            .clone()
            .into_iter()
            .map(|arg| {
                make_sub_restrictinfos(
                    root,
                    arg,
                    is_pushed_down,
                    has_clone,
                    is_clone,
                    pseudoconstant,
                    security_level,
                    required_relids.clone(),
                    incompatible_relids.clone(),
                    outer_relids.clone(),
                )
            })
            .collect();
        make_andclause(andlist)
    } else {
        Node::RestrictInfo(Box::new(make_plain_restrictinfo(
            root,
            Box::new(clause),
            None,
            is_pushed_down,
            has_clone,
            is_clone,
            pseudoconstant,
            security_level,
            required_relids,
            incompatible_relids,
            outer_relids,
        )))
    }
}

/// PG `make_plain_restrictinfo`: construct a RestrictInfo from a non-OR clause.
///
/// The relid sets are computed via `pull_varnos`: for a binary opclause the
/// left/right relids are set (and `can_join` if they're disjoint and nonempty,
/// the syntactic join-clause test); otherwise only `clause_relids` is set.
/// `required_relids` defaults to `clause_relids`. `num_base_rels` counts the
/// base rels in `clause_relids` (dropping `root.outer_join_rels`), and a fresh
/// `rinfo_serial` is assigned. This is what lets `distribute_restrictinfo_to_rels`
/// tell a base restriction (singleton) from a join clause (multiple).
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
    use crate::nodes::bitmapset::{bms_difference, bms_num_members, bms_overlap, bms_union};
    use crate::nodes::nodeFuncs::{get_leftop, get_rightop, is_opclause};

    let clause_node: Node = (*clause).clone();

    let mut can_join = false;
    let (left_relids, right_relids, clause_relids): (Option<Relids>, Option<Relids>, Option<Relids>) =
        if is_opclause(Some(&clause_node)) && opclause_is_binary(&clause_node) {
            let left = get_leftop(&clause_node).cloned();
            let right = get_rightop(&clause_node).cloned();
            let lrelids = crate::backend::optimizer::util::var::pull_varnos(root, left);
            let rrelids = crate::backend::optimizer::util::var::pull_varnos(root, right);
            let crelids = bms_union(&lrelids, &rrelids);
            // Does it look like a normal join clause? (binary op over disjoint,
            // both-nonempty relid sets) -- a purely syntactic test.
            if bms_num_members(&lrelids) != 0
                && bms_num_members(&rrelids) != 0
                && !bms_overlap(&lrelids, &rrelids)
            {
                can_join = true;
                crate::assert!(!pseudoconstant);
            }
            (Some(lrelids), Some(rrelids), Some(crelids))
        } else {
            let crelids = crate::backend::optimizer::util::var::pull_varnos(root, Some(clause_node));
            (None, None, Some(crelids))
        };

    // required_relids defaults to clause_relids.
    let required_relids = required_relids.or_else(|| clause_relids.clone());

    // Count base rels in clause_relids (delete outer_join_rels, count survivors).
    let outer_join_rels = root.outer_join_rels.clone().unwrap_or_default();
    let baserels = bms_difference(&clause_relids.clone().unwrap_or_default(), &outer_join_rels);
    let num_base_rels = bms_num_members(&baserels);

    // Fresh serial number.
    root.last_rinfo_serial += 1;
    let rinfo_serial = root.last_rinfo_serial;

    RestrictInfo {
        clause: *clause,
        orclause: orclause.map(|b| *b),
        is_pushed_down,
        pseudoconstant,
        has_clone,
        is_clone,
        can_join,
        security_level,
        incompatible_relids,
        outer_relids,
        // security_level == 0 quals are never delayed, so leakproof is "don't know".
        leakproof: false,
        has_volatile: VolatileFunctionStatus::UNKNOWN,
        clause_relids,
        left_relids,
        right_relids,
        required_relids,
        num_base_rels,
        rinfo_serial,
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
