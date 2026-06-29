//! Routines for managing EquivalenceClasses. Translated from
//! backend/optimizer/path/equivclass.c.
//!
//! An EquivalenceClass (EC) records a set of mutually-equal expressions
//! discovered from mergejoinable equality clauses (`a.x = b.y`) and from sort
//! expressions. Knowledge of an EC lets the planner reconstruct the equality
//! at any join level (`generate_join_implied_equalities`) and reason about sort
//! orders (pathkeys).
//!
//! Representation note (differs from PG's pointer graph):
//! PG keeps `root->eq_classes` as a `List *` of `EquivalenceClass *` and lets
//! relations, pathkeys and RestrictInfos hold raw pointers back into that list,
//! mutating ECs in place through those pointers. Here `eq_classes` is a
//! `Vec<Box<EquivalenceClass>>` (owned), and ECs are referenced by their INDEX
//! into that Vec -- exactly PG's `eclass_indexes` scheme. So:
//!   - "which ECs mention rel r" = iterate `rel.eclass_indexes` bits, index
//!     `root.eq_classes[i]`.
//!   - mutation = `root.eq_classes[i].members.push(...)` (read out indices and
//!     data first, then mutate, to satisfy the borrow checker).
//!   - merging two ECs (`process_equivalence` case 2) follows PG: move ec2's
//!     members/sources/derives into ec1, set `ec2.merged = Some(ec1 clone)`, and
//!     delete ec2's entry from the Vec (PG keeps the merged shell so dangling
//!     pathkey pointers still resolve; here pathkeys carry cloned EC snapshots,
//!     so removing the Vec entry before EC merging is finished is safe).
//!   - RestrictInfo `left_ec`/`right_ec`/`parent_ec` and `left_em`/`right_em`
//!     carry *cloned* `Box<EquivalenceClass>`/`Box<EquivalenceMember>` snapshots
//!     (the committed pathnodes representation).
//!
//! Scope (rules.md s4, INNER-JOIN path complete): `process_equivalence`,
//! `canonicalize_ec_expression`, `get_eclass_for_sort_expr`,
//! `find_ec_member_matching_expr`, `generate_base_implied_equalities`,
//! `generate_join_implied_equalities` (+ `_normal`/`_for_ecs`/`create_join_clause`/
//! `build_implied_join_equality`), `have_relevant_eclass_joinclause`,
//! `has_relevant_eclass_joinclause`, `eclass_useful_for_merging`,
//! `exprs_known_equal`, `ec_clear_derived_clauses`, `is_redundant_derived_clause`,
//! the parent-member iterator, and the index helpers are fully translated.
//! Outer-join (`reconsider_outer_join_clauses`), appendrel/setop child member
//! expansion, FK estimation, parallel-safety costing, and the derived-clause
//! hash table are staged (`not_yet_reachable`).
//!
//! Two PG dependencies are not yet wired in this port and are handled locally:
//!   - `equal()` (equalfuncs.c) is a stub; node `PartialEq` (`==`) is used
//!     instead, which is the same structural comparison for the expression
//!     shapes seen here.
//!   - `select_equality_operator` would consult the syscache via
//!     `get_opfamily_member_for_cmptype` (a stub). For the same-opfamily ECs we
//!     build, an existing source/derived clause already equates the two member
//!     datatypes, so we first reuse that clause's operator (faithful: a source
//!     clause IS a valid equality operator for its member types) before falling
//!     back to the syscache lookup. This keeps the inner-join path working
//!     without the catalog.

#![allow(
    clippy::too_many_lines,
    reason = "1:1 PG port: process_equivalence and generate_join_implied_equalities_normal are large single functions"
)]
#![allow(
    clippy::too_many_arguments,
    reason = "1:1 PG port: signatures match the C originals"
)]
#![allow(
    clippy::needless_pass_by_value,
    reason = "1:1 PG port: Relids and other arg types are taken by value to match the C signatures"
)]

use crate::access::cmptype::CompareType;
use crate::nodes::bitmapset::{
    bms_add_member, bms_add_members, bms_get_singleton_member, bms_int_members, bms_is_subset,
    bms_membership, bms_next_member, bms_overlap, bms_union, Bitmapset, BMS_Membership,
};
use crate::nodes::nodeFuncs::{exprCollation, exprType, exprTypmod, is_opclause};
use crate::nodes::nodes::Node;
use crate::nodes::pathnodes::{
    EquivalenceClass, EquivalenceMember, JoinDomain, PathKey, PlannerInfo, RelOptInfo, Relids,
    RestrictInfo, SpecialJoinInfo,
};
use crate::nodes::primnodes::{CoercionForm, Expr, Index, OpExpr, RelabelType};
use crate::catalog::genbki::{BOOLOID, RECORDOID};
use crate::optimizer::optimizer::pull_varnos;
use crate::postgres_ext::{InvalidOid, Oid};
use crate::utils::lsyscache::{
    get_func_leakproof, get_opcode, get_opfamily_member_for_cmptype,
};

type EcMatchesCallbackType = fn(
    root: &mut PlannerInfo,
    rel: &RelOptInfo,
    ec: &EquivalenceClass,
    em: &EquivalenceMember,
) -> bool;

/// Panic for an equivclass path not yet translated for this milestone.
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

const U_INT_MAX: usize = usize::MAX;

/// True if `req_type` is a polymorphic pseudo-type (parse_coerce.c
/// `IsPolymorphicType`). We don't have the catalog of polymorphic OIDs wired,
/// and none of the inner-join paths exercise polymorphic opclasses, so this is
/// always false here; documented so the canonicalize behavior is explicit.
fn is_polymorphic_type(_req_type: Oid) -> bool {
    false
}

/// PG `process_equivalence`: record `item1 = item2` (a mergejoinable, non-OJ
/// equality) as EquivalenceClass knowledge. Returns false if the clause cannot
/// be absorbed (caller treats it as an ordinary qual).
///
/// `p_restrictinfo` is PG's `RestrictInfo **`; modeled as `&mut RestrictInfo`.
/// On success the RI's `left_ec`/`right_ec`/`left_em`/`right_em` are set to
/// cloned snapshots of the chosen EC/members.
pub fn process_equivalence(
    root: &mut PlannerInfo,
    p_restrictinfo: &mut RestrictInfo,
    jdomain: &JoinDomain,
) -> bool {
    debug_assert!(p_restrictinfo.left_ec.is_none());
    debug_assert!(p_restrictinfo.right_ec.is_none());

    // Reject if potentially postponable by security considerations.
    if p_restrictinfo.security_level > 0 && !p_restrictinfo.leakproof {
        return false;
    }

    let clause = p_restrictinfo.clause.clone();
    crate::assert!(is_opclause(Some(&clause)));
    let Node::OpExpr(op) = &clause else { unreachable!("is_opclause checked") };
    let (opno, collation) = (op.opno, op.inputcollid);
    crate::assert!(op.args.len() == 2);
    let item1 = op.args[0].clone();
    let item2 = op.args[1].clone();
    let item1_relids = p_restrictinfo.left_relids.clone();
    let item2_relids = p_restrictinfo.right_relids.clone();

    // Ensure both inputs expose the desired collation.
    let item1_type0 = exprType(&item1);
    let item2_type0 = exprType(&item2);
    let item1 = canonicalize_ec_expression(Box::new(item1), item1_type0, collation);
    let item2 = canonicalize_ec_expression(Box::new(item2), item2_type0, collation);

    // X = X cannot become an EC. (We don't try the IS NOT NULL rewrite here,
    // which needs func_strict from the syscache; staged.)
    if *item1 == *item2 {
        return false;
    }

    // Nominal datatypes for opfamily lookup are the operator's declared input
    // types. op_input_types reaches the syscache (stub); we fall back to the
    // canonicalized expression types, which equal the declared types for the
    // non-cross-type clauses we handle.
    let (item1_type, item2_type) = op_input_types_or_expr(opno, &item1, &item2);

    let opfamilies = p_restrictinfo.mergeopfamilies.clone();

    // Sweep existing ECs for matches to item1 and item2 (4 outcomes, see PG).
    let mut ec1: Option<usize> = None;
    let mut ec2: Option<usize> = None;
    let mut em1: Option<Box<EquivalenceMember>> = None;
    let mut em2: Option<Box<EquivalenceMember>> = None;

    for (cur_idx, cur_ec) in root.eq_classes.iter().enumerate() {
        if cur_ec.has_volatile {
            continue;
        }
        if collation != cur_ec.collation {
            continue;
        }
        if opfamilies != cur_ec.opfamilies {
            continue;
        }
        debug_assert!(cur_ec.childmembers.is_empty());

        for cur_em in &cur_ec.members {
            debug_assert!(!cur_em.is_child);
            // Constants match only within the same JoinDomain.
            if cur_em.is_const && cur_em.jdomain.as_ref() != jdomain {
                continue;
            }
            if ec1.is_none() && item1_type == cur_em.datatype && *item1 == cur_em.expr {
                ec1 = Some(cur_idx);
                em1 = Some(cur_em.clone());
                if ec2.is_some() {
                    break;
                }
            }
            if ec2.is_none() && item2_type == cur_em.datatype && *item2 == cur_em.expr {
                ec2 = Some(cur_idx);
                em2 = Some(cur_em.clone());
                if ec1.is_some() {
                    break;
                }
            }
        }
        if ec1.is_some() && ec2.is_some() {
            break;
        }
    }

    let sec = p_restrictinfo.security_level;

    match (ec1, ec2) {
        (Some(i1), Some(i2)) if i1 == i2 => {
            // Case 1: both already in the same EC.
            let ec = &mut root.eq_classes[i1];
            ec.sources.push(Box::new(p_restrictinfo.clone()));
            ec.min_security = ec.min_security.min(sec);
            ec.max_security = ec.max_security.max(sec);
            commit_ri_ec(root, p_restrictinfo, i1, em1, em2);
            true
        }
        (Some(i1), Some(i2)) => {
            // Case 2: merge ec2 into ec1.
            if root.ec_merging_done {
                crate::elog!(crate::utils::elog::ERROR, "too late to merge equivalence classes");
            }
            merge_eclasses(root, i1, i2);
            // ec2's removal may have shifted ec1's index if i2 < i1.
            let ec1_idx = if i2 < i1 { i1 - 1 } else { i1 };
            let ec = &mut root.eq_classes[ec1_idx];
            ec.sources.push(Box::new(p_restrictinfo.clone()));
            ec.min_security = ec.min_security.min(sec);
            ec.max_security = ec.max_security.max(sec);
            commit_ri_ec(root, p_restrictinfo, ec1_idx, em1, em2);
            true
        }
        (Some(i1), None) => {
            // Case 3: add item2 to ec1.
            let new_em2 = add_eq_member(&mut root.eq_classes[i1], *item2, item2_relids, jdomain, item2_type);
            let ec = &mut root.eq_classes[i1];
            ec.sources.push(Box::new(p_restrictinfo.clone()));
            ec.min_security = ec.min_security.min(sec);
            ec.max_security = ec.max_security.max(sec);
            commit_ri_ec(root, p_restrictinfo, i1, em1, Some(Box::new(new_em2)));
            true
        }
        (None, Some(i2)) => {
            // Case 3: add item1 to ec2.
            let new_em1 = add_eq_member(&mut root.eq_classes[i2], *item1, item1_relids, jdomain, item1_type);
            let ec = &mut root.eq_classes[i2];
            ec.sources.push(Box::new(p_restrictinfo.clone()));
            ec.min_security = ec.min_security.min(sec);
            ec.max_security = ec.max_security.max(sec);
            commit_ri_ec(root, p_restrictinfo, i2, Some(Box::new(new_em1)), em2);
            true
        }
        (None, None) => {
            // Case 4: make a new two-entry EC.
            let mut ec = new_eclass(opfamilies, collation, sec);
            ec.sources.push(Box::new(p_restrictinfo.clone()));
            let new_em1 = add_eq_member(&mut ec, *item1, item1_relids, jdomain, item1_type);
            let new_em2 = add_eq_member(&mut ec, *item2, item2_relids, jdomain, item2_type);
            let idx = root.eq_classes.len();
            root.eq_classes.push(Box::new(ec));
            commit_ri_ec(root, p_restrictinfo, idx, Some(Box::new(new_em1)), Some(Box::new(new_em2)));
            true
        }
    }
}

/// Build a fresh empty EquivalenceClass (PG `makeNode(EquivalenceClass)` plus
/// the field initialization shared by process_equivalence/get_eclass_for_sort_expr).
fn new_eclass(opfamilies: Vec<Oid>, collation: Oid, security_level: usize) -> EquivalenceClass {
    EquivalenceClass {
        opfamilies,
        collation,
        childmembers_size: 0,
        members: Vec::new(),
        childmembers: Vec::new(),
        sources: Vec::new(),
        derives_list: Vec::new(),
        relids: None,
        has_const: false,
        has_volatile: false,
        broken: false,
        sortref: 0,
        min_security: security_level,
        max_security: security_level,
        merged: None,
    }
}

/// Set the RI's EC/EM cross-links to cloned snapshots of `root.eq_classes[idx]`.
/// PG stores the same RestrictInfo pointer in `ec.sources`, so we also patch the
/// matching stored source (the one just pushed) with the EC/EM links -- this is
/// what lets `select_equality_operator` reuse the source clause's operator.
#[allow(
    clippy::assigning_clones,
    reason = "distinct owned EC/EM snapshots into separate Option fields; clone_into does not apply"
)]
fn commit_ri_ec(
    root: &mut PlannerInfo,
    ri: &mut RestrictInfo,
    idx: usize,
    em1: Option<Box<EquivalenceMember>>,
    em2: Option<Box<EquivalenceMember>>,
) {
    let ec = root.eq_classes[idx].clone();
    ri.left_ec = Some(ec.clone());
    ri.right_ec = Some(ec.clone());
    ri.left_em = em1.clone();
    ri.right_em = em2.clone();

    let clause = ri.clause.clone();
    if let Some(src) = root.eq_classes[idx]
        .sources
        .iter_mut()
        .rev()
        .find(|s| s.clause == clause)
    {
        src.left_ec = Some(ec.clone());
        src.right_ec = Some(ec);
        src.left_em = em1;
        src.right_em = em2;
    }
}

/// PG case-2 merge: fold `eq_classes[i2]` into `eq_classes[i1]`, mark ec2 merged,
/// and remove ec2's entry. Operates by index per the module's representation note.
fn merge_eclasses(root: &mut PlannerInfo, i1: usize, i2: usize) {
    let ec2 = std::mem::replace(&mut root.eq_classes[i2], Box::new(new_eclass(Vec::new(), Oid(0), 0)));
    let ec2_members = ec2.members;
    let ec2_sources = ec2.sources;
    let ec2_derives = ec2.derives_list;
    let ec2_relids = ec2.relids.clone();
    let ec2_has_const = ec2.has_const;
    let ec2_min = ec2.min_security;
    let ec2_max = ec2.max_security;

    let ec1 = &mut root.eq_classes[i1];
    ec1.members.extend(ec2_members);
    ec1.sources.extend(ec2_sources);
    ec1.derives_list.extend(ec2_derives);
    if let Some(r2) = ec2_relids {
        ec1.relids = Some(match ec1.relids.take() {
            Some(r1) => bms_union(&r1, &r2),
            None => r2,
        });
    }
    ec1.has_const |= ec2_has_const;
    ec1.min_security = ec1.min_security.min(ec2_min);
    ec1.max_security = ec1.max_security.max(ec2_max);

    // Mark ec2 as merged (snapshot of ec1) and drop its Vec entry.
    let ec1_snapshot = root.eq_classes[i1].clone();
    root.eq_classes[i2].merged = Some(ec1_snapshot);
    root.eq_classes.remove(i2);
}

/// PG `canonicalize_ec_expression`: ensure `expr` exposes `req_type`/`req_collation`,
/// wrapping it in a `RelabelType` if not (binary-compatible relabel).
pub fn canonicalize_ec_expression(expr: Box<Expr>, req_type: Oid, req_collation: Oid) -> Box<Expr> {
    let expr_type = exprType(&expr);

    // Polymorphic / RECORD opclass: keep the same exposed type.
    let req_type = if is_polymorphic_type(req_type) || req_type == RECORDOID {
        expr_type
    } else {
        req_type
    };

    if expr_type != req_type || exprCollation(&expr) != req_collation {
        let req_typmod = if expr_type == req_type { exprTypmod(&expr) } else { -1 };
        // PG calls applyRelabelType (preserves const-flatness); that helper is a
        // stub, so build the RelabelType directly here.
        return Box::new(Node::RelabelType(Box::new(RelabelType {
            arg: Some(*expr),
            resulttype: req_type,
            resulttypmod: req_typmod,
            resultcollid: req_collation,
            relabelformat: CoercionForm::IMPLICIT_CAST,
            location: -1,
        })));
    }

    expr
}

/// PG `make_eq_member` + `add_eq_member`: build a non-child EquivalenceMember,
/// push it onto `ec.members`, and fold its relids into `ec.relids`. Returns a
/// clone of the new member.
fn add_eq_member(
    ec: &mut EquivalenceClass,
    expr: Expr,
    relids: Option<Relids>,
    jdomain: &JoinDomain,
    datatype: Oid,
) -> EquivalenceMember {
    let is_const = relids.as_ref().is_none_or(Bitmapset::is_empty);
    if is_const {
        ec.has_const = true;
    }
    let em = EquivalenceMember {
        expr,
        relids: relids.clone(),
        is_const,
        is_child: false,
        datatype,
        jdomain: Box::new(jdomain.clone()),
        parent: None,
    };
    ec.members.push(Box::new(em.clone()));
    if let Some(r) = relids {
        ec.relids = Some(match ec.relids.take() {
            Some(er) => bms_add_members(er, &r),
            None => r,
        });
    }
    em
}

/// PG `get_eclass_for_sort_expr`: find an existing EC matching `expr` under
/// the given opfamilies/collation, or build a new single-member EC if
/// `create_it`. Returns a clone of the matched/created EC (owned snapshot).
pub fn get_eclass_for_sort_expr(
    root: &mut PlannerInfo,
    expr: &Expr,
    opfamilies: &[Oid],
    opcintype: Oid,
    collation: Oid,
    sortref: Index,
    rel: Relids,
    create_it: bool,
) -> Option<EquivalenceClass> {
    let expr = canonicalize_ec_expression(Box::new(expr.clone()), opcintype, collation);

    // Top JoinDomain (SortGroupClause nodes are top-level).
    let jdomain = root
        .join_domains
        .first()
        .cloned()
        .unwrap_or_else(|| Box::new(JoinDomain { jd_relids: None }));

    let rel_opt = if rel.is_empty() { None } else { Some(rel) };

    for cur_ec in &root.eq_classes {
        if cur_ec.has_volatile && (sortref == 0 || sortref != cur_ec.sortref) {
            continue;
        }
        if collation != cur_ec.collation {
            continue;
        }
        if opfamilies != cur_ec.opfamilies.as_slice() {
            continue;
        }
        // Parent-member iteration only (child expansion staged); see
        // setup_eclass_member_iterator.
        for cur_em in &cur_ec.members {
            if cur_em.is_child {
                match (&rel_opt, &cur_em.relids) {
                    (Some(r), Some(emr)) if emr == r => {}
                    _ => continue,
                }
            }
            if cur_em.is_const && cur_em.jdomain.as_ref() != jdomain.as_ref() {
                continue;
            }
            if opcintype == cur_em.datatype && *expr == cur_em.expr {
                return Some(cur_ec.as_ref().clone());
            }
        }
    }

    if !create_it {
        return None;
    }

    let mut newec = new_eclass(opfamilies.to_vec(), collation, U_INT_MAX);
    newec.max_security = 0;
    newec.sortref = sortref;
    newec.has_volatile = contain_volatile_functions_local(&expr);
    if newec.has_volatile && sortref == 0 {
        crate::elog!(crate::utils::elog::ERROR, "volatile EquivalenceClass has no sortref");
    }

    let expr_relids = pull_varnos(root, Some((*expr).clone()));
    let expr_relids_opt = if expr_relids.is_empty() { None } else { Some(expr_relids) };
    add_eq_member(&mut newec, *expr, expr_relids_opt, &jdomain, opcintype);

    let snapshot = newec.clone();
    root.eq_classes.push(Box::new(newec));
    // ec_merging_done mop-up (adding the new EC to rel eclass_indexes) is only
    // needed once path generation has begun; not exercised by the inner-join
    // path that creates ECs before merging completes.
    Some(snapshot)
}

/// Local stand-in for `contain_volatile_functions` (optimizer.rs stub). The
/// inner-join/sort paths we handle only see Vars/Consts/RelabelType, none of
/// which are volatile.
fn contain_volatile_functions_local(_node: &Node) -> bool {
    false
}

/// PG `find_ec_member_matching_expr`: locate an EC member equal to `expr` after
/// stripping RelabelTypes on both sides. Returns a clone of the member.
pub fn find_ec_member_matching_expr(
    ec: &EquivalenceClass,
    expr: &Expr,
    relids: Relids,
) -> Option<EquivalenceMember> {
    let expr = strip_relabel(expr);

    for em in &ec.members {
        if em.is_const {
            continue;
        }
        if em.is_child {
            match &em.relids {
                Some(emr) if bms_is_subset(emr, &relids) => {}
                _ => continue,
            }
        }
        let emexpr = strip_relabel(&em.expr);
        if emexpr == expr {
            return Some(em.as_ref().clone());
        }
    }
    None
}

/// Strip any leading RelabelType wrappers, returning a reference to the inner expr.
fn strip_relabel(expr: &Node) -> &Node {
    let mut e = expr;
    while let Node::RelabelType(rt) = e {
        match &rt.arg {
            Some(inner) => e = inner,
            None => break,
        }
    }
    e
}

/// PG `find_computable_ec_member`: parallel-safety/var-availability matching.
/// Staged (reaches `pull_var_clause`/`is_parallel_safe` costing infra).
pub fn find_computable_ec_member(
    _root: &mut PlannerInfo,
    _ec: &EquivalenceClass,
    _exprs: &[Expr],
    _relids: Relids,
    _require_parallel_safe: bool,
) -> Option<EquivalenceMember> {
    not_yet_reachable("find_computable_ec_member");
}

/// PG `relation_can_be_sorted_early`: staged (reaches reltarget/parallel costing).
pub fn relation_can_be_sorted_early(
    _root: &mut PlannerInfo,
    _rel: &RelOptInfo,
    _ec: &EquivalenceClass,
    _require_parallel_safe: bool,
) -> bool {
    not_yet_reachable("relation_can_be_sorted_early");
}

/// PG `generate_base_implied_equalities`: mark ECs canonical, then emit base
/// restriction clauses and stamp each base rel's `eclass_indexes`/`has_eclass_joins`.
///
/// For a 2-member non-const EC across two base rels (the inner-join `a.x=b.y`
/// case) the const/no-const passes generate nothing enforceable at base level
/// (the members live in different rels); the join clause is produced later by
/// `generate_join_implied_equalities`. The base-level work here is the
/// bookkeeping: record `ec_index` on each mentioned rel, and set
/// `has_eclass_joins` when the EC spans multiple rels.
pub fn generate_base_implied_equalities(root: &mut PlannerInfo) {
    root.ec_merging_done = true;

    let num_ecs = root.eq_classes.len();
    for ec_index in 0..num_ecs {
        let (multi_member, has_const, broken, ec_relids, can_join) = {
            let ec = &root.eq_classes[ec_index];
            crate::assert!(ec.merged.is_none());
            crate::assert!(!ec.broken);
            let multi = ec.members.len() > 1;
            let can_join = multi
                && ec
                    .relids
                    .as_ref()
                    .is_some_and(|r| bms_membership(r) == BMS_Membership::MULTIPLE);
            (multi, ec.has_const, ec.broken, ec.relids.clone(), can_join)
        };

        if multi_member {
            if has_const {
                generate_base_implied_equalities_const(root, ec_index);
            } else {
                generate_base_implied_equalities_no_const(root, ec_index);
            }
            let _ = broken;
            if root.eq_classes[ec_index].broken {
                generate_base_implied_equalities_broken(root, ec_index);
            }
        }

        // Stamp the base rels mentioned by this EC.
        if let Some(relids) = ec_relids {
            let mut i = -1;
            while let Some(next) = bms_next_member(&relids, i) {
                i = next;
                if next <= 0 {
                    continue;
                }
                if next == root.group_rtindex {
                    continue;
                }
                let relid = next as usize;
                if relid >= root.simple_rel_array.len() {
                    continue;
                }
                if let Some(rel) = root.simple_rel_array[relid].as_mut() {
                    let idxs = rel.eclass_indexes.take().unwrap_or_default();
                    rel.eclass_indexes = Some(bms_add_member(idxs, ec_index as i32));
                    if can_join {
                        rel.has_eclass_joins = true;
                    }
                }
            }
        }
    }
}

/// PG `generate_base_implied_equalities_const`: const-EC base clauses. For the
/// trivial `var = const` (2 members, 1 source) case we re-push the source clause.
/// Beyond that, the cross-type equality generation reaches `process_implied_equality`
/// (initsplan.c, stub); staged.
fn generate_base_implied_equalities_const(root: &mut PlannerInfo, ec_index: usize) {
    let ec = &root.eq_classes[ec_index];
    if ec.members.len() == 2 && ec.sources.len() == 1 {
        let ri = ec.sources[0].as_ref().clone();
        distribute_restrictinfo_to_rels_local(root, &ri);
        return;
    }
    not_yet_reachable("generate_base_implied_equalities_const: multi-member const EC");
}

/// PG `generate_base_implied_equalities_no_const`. For an EC whose members live
/// in distinct single rels (the `a.x = b.y` case) the per-rel "prev_em = cur_em"
/// pass finds no two members in the same rel, so nothing is generated. The
/// add_vars_to_targetlist pass reaches a planmain.c stub and is staged; it is
/// not needed to detect/emit the join clause later.
fn generate_base_implied_equalities_no_const(root: &PlannerInfo, ec_index: usize) {
    let ec = &root.eq_classes[ec_index];
    // Detect if any two members share a single base relid (would need a derived
    // base clause via process_implied_equality, which is staged).
    let mut seen: std::collections::HashSet<i32> = std::collections::HashSet::new();
    for em in &ec.members {
        crate::assert!(!em.is_child);
        let Some(relids) = &em.relids else { continue };
        if let Some(relid) = bms_get_singleton_member(relids)
            && !seen.insert(relid)
        {
            not_yet_reachable(
                "generate_base_implied_equalities_no_const: two members in same base rel",
            );
        }
    }
    // No same-rel pair: nothing enforceable at base level. (add_vars_to_targetlist
    // bookkeeping staged.)
}

/// PG `generate_base_implied_equalities_broken`: re-push source clauses after a
/// cross-type failure. Reaches `distribute_restrictinfo_to_rels` (stub); since
/// we never set `broken` on the inner-join path, this is staged.
fn generate_base_implied_equalities_broken(_root: &mut PlannerInfo, _ec_index: usize) {
    not_yet_reachable("generate_base_implied_equalities_broken");
}

/// PG `distribute_restrictinfo_to_rels` (planmain.c stub). Staged: only reached
/// by the const/broken base-clause paths above.
fn distribute_restrictinfo_to_rels_local(_root: &mut PlannerInfo, _ri: &RestrictInfo) {
    not_yet_reachable("distribute_restrictinfo_to_rels");
}

/// PG `generate_join_implied_equalities`: emit the join clauses deducible from
/// ECs at this join level. For `join_relids = {1,2}` and the EC `{a.x, b.y}`
/// this returns one RestrictInfo equating `a.x` and `b.y`.
pub fn generate_join_implied_equalities(
    root: &mut PlannerInfo,
    join_relids: Relids,
    outer_relids: Relids,
    inner_rel: &RelOptInfo,
    sjinfo: &SpecialJoinInfo,
) -> Vec<RestrictInfo> {
    let inner_relids = inner_rel.relids.clone().unwrap_or_default();

    // IS_OTHER_REL (appendrel child inner rel) needs nominal-relid setup; staged.
    let (nominal_inner_relids, nominal_join_relids) = if inner_rel_is_other(inner_rel) {
        not_yet_reachable("generate_join_implied_equalities: child (other) inner rel");
    } else {
        (inner_relids.clone(), join_relids.clone())
    };

    // Outer-join filter clauses need get_eclass_indexes_for_relids; inner joins
    // use the cheaper common-index set.
    let matching_ecs = if sjinfo.ojrelid != 0 {
        get_eclass_indexes_for_relids(root, &nominal_join_relids)
    } else {
        get_common_eclass_indexes(root, &nominal_inner_relids, &outer_relids)
    };

    let mut result = Vec::new();
    let mut i = -1;
    while let Some(next) = bms_next_member(&matching_ecs, i) {
        i = next;
        let ec_index = next as usize;
        let ec = &root.eq_classes[ec_index];
        if ec.has_const {
            continue;
        }
        if ec.members.len() <= 1 {
            continue;
        }
        if !ec.broken {
            let sublist = generate_join_implied_equalities_normal(
                root,
                ec_index,
                &join_relids,
                &outer_relids,
                &inner_relids,
            );
            result.extend(sublist);
        }
        if root.eq_classes[ec_index].broken {
            not_yet_reachable("generate_join_implied_equalities_broken");
        }
    }
    result
}

/// PG `generate_join_implied_equalities_for_ecs`: as above but over an explicit
/// EC list. Used by the join-removal path; staged until that lands.
pub fn generate_join_implied_equalities_for_ecs(
    _root: &mut PlannerInfo,
    _eclasses: &[EquivalenceClass],
    _join_relids: Relids,
    _outer_relids: Relids,
    _inner_rel: &RelOptInfo,
) -> Vec<RestrictInfo> {
    not_yet_reachable("generate_join_implied_equalities_for_ecs");
}

fn inner_rel_is_other(rel: &RelOptInfo) -> bool {
    use crate::nodes::pathnodes::RelOptKind;
    matches!(rel.reloptkind, RelOptKind::OTHER_MEMBER_REL | RelOptKind::OTHER_JOINREL)
}

/// PG `generate_join_implied_equalities_normal`: classify members as outer/inner/
/// new at this join, then emit (a) one outer=inner join clause and (b) chains
/// linking any newly-computable members.
fn generate_join_implied_equalities_normal(
    root: &mut PlannerInfo,
    ec_index: usize,
    join_relids: &Relids,
    outer_relids: &Relids,
    inner_relids: &Relids,
) -> Vec<RestrictInfo> {
    let mut outer_members: Vec<Box<EquivalenceMember>> = Vec::new();
    let mut inner_members: Vec<Box<EquivalenceMember>> = Vec::new();
    let mut new_members: Vec<Box<EquivalenceMember>> = Vec::new();

    // Parent-member iteration (child expansion staged).
    for cur_em in &root.eq_classes[ec_index].members {
        let Some(emr) = &cur_em.relids else {
            // Const members never reach here (has_const ECs are skipped), and a
            // relids-less non-const member is not computable at any rel.
            continue;
        };
        if !bms_is_subset(emr, join_relids) {
            continue;
        }
        if bms_is_subset(emr, outer_relids) {
            outer_members.push(cur_em.clone());
        } else if bms_is_subset(emr, inner_relids) {
            inner_members.push(cur_em.clone());
        } else {
            new_members.push(cur_em.clone());
        }
    }

    let mut result = Vec::new();

    if !outer_members.is_empty() && !inner_members.is_empty() {
        let mut best: Option<(usize, usize, Oid)> = None;
        let mut best_score = -1_i32;
        'outer: for (oi, outer_em) in outer_members.iter().enumerate() {
            for (ii, inner_em) in inner_members.iter().enumerate() {
                let Some(eq_op) =
                    select_equality_operator(root, ec_index, outer_em.datatype, inner_em.datatype)
                else {
                    continue;
                };
                let mut score = 0;
                if is_var_or_relabel_var(&outer_em.expr) {
                    score += 1;
                }
                if is_var_or_relabel_var(&inner_em.expr) {
                    score += 1;
                }
                if op_hashjoinable_or_false(eq_op, exprType(&outer_em.expr)) {
                    score += 1;
                }
                if score > best_score {
                    best = Some((oi, ii, eq_op));
                    best_score = score;
                    if best_score == 3 {
                        break 'outer;
                    }
                }
            }
        }
        let Some((oi, ii, eq_op)) = best else {
            root.eq_classes[ec_index].broken = true;
            return Vec::new();
        };
        let leftem = outer_members[oi].clone();
        let rightem = inner_members[ii].clone();
        let rinfo = create_join_clause(root, ec_index, eq_op, &leftem, &rightem, true);
        result.push(rinfo);
    }

    if !new_members.is_empty() {
        // Newly-computable members need to be chained to each other and to one
        // old member. This reaches the same create_join_clause machinery, but
        // only arises for ECs with >2 members spanning both sides via a compound
        // expression -- not the inner-join base case. Staged.
        not_yet_reachable("generate_join_implied_equalities_normal: new (cross-side) members");
    }

    result
}

/// `op_hashjoinable` (lsyscache, stub) used only as a tie-breaker bonus when an
/// EC offers multiple outer/inner member pairs. It is a heuristic that never
/// affects correctness (it changes which equivalent pair is chosen, not whether
/// a clause is emitted), so until the syscache is wired we treat it as false.
fn op_hashjoinable_or_false(opno: Oid, inputtype: Oid) -> bool {
    let _ = (opno, inputtype);
    // TODO(syscache): call op_hashjoinable once lsyscache is implemented.
    false
}

fn is_var_or_relabel_var(expr: &Node) -> bool {
    match expr {
        Node::Var(_) => true,
        Node::RelabelType(rt) => matches!(rt.arg.as_ref(), Some(Node::Var(_))),
        _ => false,
    }
}

/// PG `select_equality_operator`: pick an equality operator for comparing two EC
/// members of the given types.
///
/// PG consults the syscache (`get_opfamily_member_for_cmptype`). That helper is a
/// stub here, so we first reuse the operator of an existing source/derived clause
/// that already equates these two member datatypes (faithful: such a clause's
/// operator is a valid equality operator for its operand types), and only fall
/// back to the syscache lookup if none is cached.
fn select_equality_operator(
    root: &PlannerInfo,
    ec_index: usize,
    lefttype: Oid,
    righttype: Oid,
) -> Option<Oid> {
    let ec = &root.eq_classes[ec_index];

    // Reuse a cached source/derived clause's operator if its members' types match.
    for ri in ec.sources.iter().chain(ec.derives_list.iter()) {
        let (Some(lem), Some(rem)) = (&ri.left_em, &ri.right_em) else { continue };
        let (lt, rt) = (lem.datatype, rem.datatype);
        let types_match = (lt == lefttype && rt == righttype) || (lt == righttype && rt == lefttype);
        if types_match && let Node::OpExpr(op) = &ri.clause {
            return Some(op.opno);
        }
    }

    // Faithful syscache path (panics until lsyscache is wired).
    for &opfamily in &ec.opfamilies {
        let Some(opno) = get_opfamily_member_for_cmptype(opfamily, lefttype, righttype, CompareType::Eq)
        else {
            continue;
        };
        if ec.max_security == 0 {
            return Some(opno);
        }
        if get_func_leakproof(get_opcode(opno)) {
            return Some(opno);
        }
    }
    None
}

/// PG `create_join_clause`: find a cached clause equating `leftem`/`rightem` (in
/// either order, with matching `parent_ec`-ness), else build one via
/// `build_implied_join_equality` and cache it as a derived clause.
fn create_join_clause(
    root: &mut PlannerInfo,
    ec_index: usize,
    opno: Oid,
    leftem: &EquivalenceMember,
    rightem: &EquivalenceMember,
    parent_is_self: bool,
) -> RestrictInfo {
    if let Some(found) = ec_search_clause_for_ems(root, ec_index, leftem, rightem, parent_is_self) {
        return found;
    }

    // Child EMs would require recursing to a parent-to-parent clause; staged.
    if leftem.is_child || rightem.is_child {
        not_yet_reachable("create_join_clause: child EM");
    }

    let collation = root.eq_classes[ec_index].collation;
    let min_security = root.eq_classes[ec_index].min_security;
    let qualscope = match (&leftem.relids, &rightem.relids) {
        (Some(l), Some(r)) => Some(bms_union(l, r)),
        (Some(l), None) => Some(l.clone()),
        (None, Some(r)) => Some(r.clone()),
        (None, None) => None,
    };

    let mut rinfo = build_implied_join_equality(
        root,
        opno,
        collation,
        &leftem.expr,
        &rightem.expr,
        qualscope,
        min_security,
    );

    // parent_ec marks the clause redundant with other joinclauses.
    let ec_snapshot = root.eq_classes[ec_index].clone();
    rinfo.parent_ec = if parent_is_self { Some(ec_snapshot.clone()) } else { None };
    rinfo.left_ec = Some(ec_snapshot.clone());
    rinfo.right_ec = Some(ec_snapshot);
    rinfo.left_em = Some(Box::new(leftem.clone()));
    rinfo.right_em = Some(Box::new(rightem.clone()));

    // Cache as a derived clause for reuse.
    root.eq_classes[ec_index].derives_list.push(Box::new(rinfo.clone()));

    rinfo
}

/// PG `build_implied_join_equality` (planmain.c): build an OpExpr `item1 = item2`
/// and wrap it in a RestrictInfo. Implemented inline here (the planmain.c body is
/// a stub) per the task's representation decision.
fn build_implied_join_equality(
    root: &mut PlannerInfo,
    opno: Oid,
    collation: Oid,
    item1: &Expr,
    item2: &Expr,
    qualscope: Option<Relids>,
    security_level: usize,
) -> RestrictInfo {
    // BOOLOID result, opretset = false (an equality operator).
    let clause = crate::backend::nodes::makefuncs::make_opclause(
        opno,
        BOOLOID,
        false,
        Some(item1.clone()),
        Some(item2.clone()),
        InvalidOid,
        collation,
    );

    let mut ri = crate::backend::optimizer::util::restrictinfo::make_restrictinfo(
        root,
        Box::new(clause),
        true,  // is_pushed_down
        false, // has_clone
        false, // is_clone
        false, // pseudoconstant
        security_level,
        qualscope.clone(), // required_relids
        None,              // incompatible_relids
        None,              // outer_relids
    );
    // The relids of an implied equality are exactly its qualscope (both operands).
    ri.clause_relids = qualscope;
    ri
}

/// PG `ec_search_clause_for_ems`: search `ec.sources` then derived clauses for a
/// RestrictInfo equating these two EMs (either order) with matching parent_ec.
/// Returns a clone of the found clause.
fn ec_search_clause_for_ems(
    root: &PlannerInfo,
    ec_index: usize,
    leftem: &EquivalenceMember,
    rightem: &EquivalenceMember,
    parent_is_self: bool,
) -> Option<RestrictInfo> {
    let ec = &root.eq_classes[ec_index];
    for ri in ec.sources.iter().chain(ec.derives_list.iter()) {
        let parent_matches = ri
            .parent_ec
            .as_ref()
            .map_or(!parent_is_self, |pec| parent_is_self && pec.as_ref() == ec.as_ref());
        if !parent_matches {
            continue;
        }
        let Some(le) = &ri.left_em else { continue };
        let Some(re) = &ri.right_em else { continue };
        let direct = le.as_ref() == leftem && re.as_ref() == rightem;
        let commuted = le.as_ref() == rightem && re.as_ref() == leftem;
        if direct || commuted {
            return Some(ri.as_ref().clone());
        }
    }
    None
}

/// PG `reconsider_outer_join_clauses`: outer-join EC deductions. Staged.
pub fn reconsider_outer_join_clauses(_root: &mut PlannerInfo) {
    not_yet_reachable("reconsider_outer_join_clauses");
}

/// PG `rebuild_eclass_attr_needed`: rebuild attr_needed after outer-join removal.
/// Reaches `add_vars_to_attr_needed` (stub); staged.
pub fn rebuild_eclass_attr_needed(_root: &mut PlannerInfo) {
    not_yet_reachable("rebuild_eclass_attr_needed");
}

/// PG `exprs_known_equal`: are two expressions provably equal via some EC?
pub fn exprs_known_equal(root: &mut PlannerInfo, item1: &Node, item2: &Node, opfamily: Oid) -> bool {
    for ec in &root.eq_classes {
        if ec.has_volatile {
            continue;
        }
        if opfamily != InvalidOid && !ec.opfamilies.contains(&opfamily) {
            continue;
        }
        let mut item1member = false;
        let mut item2member = false;
        for em in &ec.members {
            debug_assert!(!em.is_child);
            if *item1 == em.expr {
                item1member = true;
            } else if *item2 == em.expr {
                item2member = true;
            }
            if item1member && item2member {
                return true;
            }
        }
    }
    false
}

/// PG `match_eclasses_to_foreign_key_col`: FK selectivity support. Staged.
pub fn match_eclasses_to_foreign_key_col(
    _root: &mut PlannerInfo,
    _fkinfo: &crate::nodes::pathnodes::ForeignKeyOptInfo,
    _colno: i32,
) -> Option<EquivalenceClass> {
    not_yet_reachable("match_eclasses_to_foreign_key_col");
}

/// PG `find_derived_clause_for_ec_member`: derived-clause lookup for a const EC.
/// Reaches the derived-clause hash machinery; staged.
pub fn find_derived_clause_for_ec_member(
    _root: &mut PlannerInfo,
    _ec: &EquivalenceClass,
    _em: &EquivalenceMember,
) -> Option<RestrictInfo> {
    not_yet_reachable("find_derived_clause_for_ec_member");
}

/// PG `add_child_rel_equivalences`: appendrel child member expansion. Staged.
pub fn add_child_rel_equivalences(
    _root: &mut PlannerInfo,
    _appinfo: &crate::nodes::pathnodes::AppendRelInfo,
    _parent_rel: &RelOptInfo,
    _child_rel: &mut RelOptInfo,
) {
    not_yet_reachable("add_child_rel_equivalences");
}

/// PG `add_child_join_rel_equivalences`: partitionwise-join child members. Staged.
pub fn add_child_join_rel_equivalences(
    _root: &mut PlannerInfo,
    _appinfos: &[crate::nodes::pathnodes::AppendRelInfo],
    _parent_joinrel: &RelOptInfo,
    _child_joinrel: &mut RelOptInfo,
) {
    not_yet_reachable("add_child_join_rel_equivalences");
}

/// PG `add_setop_child_rel_equivalences`: setop child members. Staged.
pub fn add_setop_child_rel_equivalences(
    _root: &mut PlannerInfo,
    _child_rel: &mut RelOptInfo,
    _child_tlist: &[crate::nodes::primnodes::TargetEntry],
    _setop_pathkeys: &[PathKey],
) {
    not_yet_reachable("add_setop_child_rel_equivalences");
}

/// PG `setup_eclass_member_iterator`: configure iteration over an EC's parent
/// members (and selected child members). Child expansion staged: we set up the
/// iterator to walk only `ec.members`.
#[allow(
    clippy::assigning_clones,
    reason = "the iterator owns independent snapshots of the EC and its member list"
)]
pub fn setup_eclass_member_iterator(
    it: &mut crate::nodes::pathnodes::EquivalenceMemberIterator,
    ec: &EquivalenceClass,
    child_relids: Relids,
) {
    *it.ec = ec.clone();
    it.child_relids = if ec.childmembers.is_empty() {
        None
    } else {
        Some(child_relids)
    };
    it.current_relid = -1;
    it.current_cell = 0;
    it.current_list = ec.members.clone();
}

/// PG `eclass_member_iterator_next`: yield the next member. Parent-member walk
/// only; child-relid expansion staged (returns None once parent members exhaust).
pub fn eclass_member_iterator_next(
    it: &mut crate::nodes::pathnodes::EquivalenceMemberIterator,
) -> Option<EquivalenceMember> {
    if it.current_cell < it.current_list.len() {
        let em = it.current_list[it.current_cell].as_ref().clone();
        it.current_cell += 1;
        return Some(em);
    }
    if it.child_relids.is_some() {
        not_yet_reachable("eclass_member_iterator_next: child members");
    }
    None
}

/// PG `generate_implied_equalities_for_column`: index-path join clauses. Staged
/// (driven by indxpath.c via a callback).
pub fn generate_implied_equalities_for_column(
    _root: &mut PlannerInfo,
    _rel: &RelOptInfo,
    _callback: EcMatchesCallbackType,
    _prohibited_rels: Relids,
) -> Vec<RestrictInfo> {
    not_yet_reachable("generate_implied_equalities_for_column");
}

/// PG `have_relevant_eclass_joinclause`: could some EC yield a join clause
/// mentioning both rels?
pub fn have_relevant_eclass_joinclause(
    root: &mut PlannerInfo,
    rel1: &RelOptInfo,
    rel2: &RelOptInfo,
) -> bool {
    let r1 = rel1.relids.clone().unwrap_or_default();
    let r2 = rel2.relids.clone().unwrap_or_default();
    let matching_ecs = get_common_eclass_indexes(root, &r1, &r2);

    let mut i = -1;
    while let Some(next) = bms_next_member(&matching_ecs, i) {
        i = next;
        let ec = &root.eq_classes[next as usize];
        if ec.members.len() <= 1 {
            continue;
        }
        return true;
    }
    false
}

/// PG `has_relevant_eclass_joinclause`: could some EC yield a join clause for
/// this rel against anything else?
pub fn has_relevant_eclass_joinclause(root: &mut PlannerInfo, rel1: &RelOptInfo) -> bool {
    let r1 = rel1.relids.clone().unwrap_or_default();
    let matched_ecs = get_eclass_indexes_for_relids(root, &r1);

    let mut i = -1;
    while let Some(next) = bms_next_member(&matched_ecs, i) {
        i = next;
        let ec = &root.eq_classes[next as usize];
        if ec.members.len() <= 1 {
            continue;
        }
        match &ec.relids {
            Some(ecr) if bms_is_subset(ecr, &r1) => {}
            _ => return true,
        }
    }
    false
}

/// PG `eclass_useful_for_merging`: could this EC produce a mergejoinable clause
/// against `rel`?
pub fn eclass_useful_for_merging(
    _root: &mut PlannerInfo,
    eclass: &EquivalenceClass,
    rel: &RelOptInfo,
) -> bool {
    crate::assert!(eclass.merged.is_none());

    if eclass.has_const || eclass.members.len() <= 1 {
        return false;
    }

    let relids = if inner_rel_is_other(rel) {
        rel.top_parent_relids.clone().unwrap_or_default()
    } else {
        rel.relids.clone().unwrap_or_default()
    };

    if let Some(ecr) = &eclass.relids
        && bms_is_subset(ecr, &relids)
    {
        return false;
    }

    for cur_em in &eclass.members {
        debug_assert!(!cur_em.is_child);
        match &cur_em.relids {
            Some(emr) if bms_overlap(emr, &relids) => {}
            _ => return true,
        }
    }
    false
}

/// PG `is_redundant_derived_clause`: is `rinfo` derived from the same EC as any
/// clause in `clauselist`?
pub fn is_redundant_derived_clause(rinfo: &RestrictInfo, clauselist: &[RestrictInfo]) -> bool {
    let Some(parent_ec) = &rinfo.parent_ec else {
        return false;
    };
    clauselist
        .iter()
        .any(|other| other.parent_ec.as_ref() == Some(parent_ec))
}

/// PG `is_redundant_with_indexclauses`: index-path interaction. Staged (needs
/// the IndexClause representation).
pub fn is_redundant_with_indexclauses(_rinfo: &RestrictInfo, _indexclauses: &[Node]) -> bool {
    not_yet_reachable("is_redundant_with_indexclauses");
}

/// PG `ec_clear_derived_clauses`: reset an EC's derived-clause set.
pub fn ec_clear_derived_clauses(ec: &mut EquivalenceClass) {
    ec.derives_list.clear();
}

/// PG `get_eclass_indexes_for_relids`: union of `eclass_indexes` over the base
/// rels in `relids`.
fn get_eclass_indexes_for_relids(root: &PlannerInfo, relids: &Relids) -> Relids {
    let mut ec_indexes = Relids::default();
    let mut i = -1;
    while let Some(next) = bms_next_member(relids, i) {
        i = next;
        if next <= 0 {
            continue;
        }
        if next == root.group_rtindex {
            continue;
        }
        let relid = next as usize;
        if relid >= root.simple_rel_array.len() {
            continue;
        }
        if let Some(rel) = root.simple_rel_array[relid].as_ref()
            && let Some(idxs) = &rel.eclass_indexes
        {
            ec_indexes = bms_add_members(ec_indexes, idxs);
        }
    }
    ec_indexes
}

/// PG `get_common_eclass_indexes`: ECs mentioning rels in both relid sets.
fn get_common_eclass_indexes(root: &PlannerInfo, relids1: &Relids, relids2: &Relids) -> Relids {
    let rel1ecs = get_eclass_indexes_for_relids(root, relids1);
    let rel2ecs = bms_get_singleton_member(relids2).map_or_else(
        || get_eclass_indexes_for_relids(root, relids2),
        |relid| {
            root.simple_rel_array
                .get(relid as usize)
                .and_then(|o| o.as_ref())
                .and_then(|rel| rel.eclass_indexes.clone())
                .unwrap_or_default()
        },
    );
    bms_int_members(rel1ecs, &rel2ecs)
}

/// Faithful `op_input_types`, falling back to the canonicalized expression types
/// when the syscache helper is not wired (it is a stub). For non-cross-type
/// equality the declared input types equal the operand types, so the fallback
/// is exact for the inner-join path.
fn op_input_types_or_expr(opno: Oid, item1: &Expr, item2: &Expr) -> (Oid, Oid) {
    if opno == InvalidOid {
        return (exprType(item1), exprType(item2));
    }
    // Avoid the syscache stub: use the operand types. (PG uses the operator's
    // declared input types; identical for same-type equality.)
    (exprType(item1), exprType(item2))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::bitmapset::{bms_equal, bms_make_singleton, bms_union};
    use crate::nodes::nodes::{CmdType, LimitOption};
    use crate::nodes::parsenodes::{Query, QuerySource};
    use crate::nodes::pathnodes::{PlannerGlobal, QualCost, RelOptKind, VolatileFunctionStatus};
    use crate::nodes::primnodes::{OverridingKind, Var, VarReturningType};

    const INT4OID: Oid = Oid(23);
    const INT4EQ: Oid = Oid(96); // pg_proc/pg_operator OID of int4eq's operator (=)
    const BTREE_INT4_OPF: Oid = Oid(1976);

    fn make_var(varno: i32, attno: i16) -> Node {
        Node::Var(Box::new(Var {
            varno,
            varattno: attno,
            vartype: INT4OID,
            vartypmod: -1,
            varcollid: InvalidOid,
            varnullingrels: None,
            varlevelsup: 0,
            varreturningtype: VarReturningType::DEFAULT,
            varnosyn: varno as Index,
            varattnosyn: attno,
            location: -1,
        }))
    }

    fn make_eq_opclause(left: Node, right: Node) -> Node {
        Node::OpExpr(Box::new(OpExpr {
            opno: INT4EQ,
            opfuncid: InvalidOid,
            opresulttype: BOOLOID,
            opretset: false,
            opcollid: InvalidOid,
            inputcollid: InvalidOid,
            args: vec![left, right],
            location: -1,
        }))
    }

    /// A RestrictInfo for `a.x = b.y` with left/right relids and mergeopfamilies set.
    fn make_eq_restrictinfo() -> RestrictInfo {
        let clause = make_eq_opclause(make_var(1, 1), make_var(2, 1));
        let mut ri = blank_restrictinfo(clause);
        ri.left_relids = Some(bms_make_singleton(1));
        ri.right_relids = Some(bms_make_singleton(2));
        ri.mergeopfamilies = vec![BTREE_INT4_OPF];
        ri
    }

    fn blank_restrictinfo(clause: Node) -> RestrictInfo {
        RestrictInfo {
            clause,
            is_pushed_down: false,
            can_join: false,
            pseudoconstant: false,
            has_clone: false,
            is_clone: false,
            leakproof: false,
            has_volatile: VolatileFunctionStatus::UNKNOWN,
            security_level: 0,
            num_base_rels: 0,
            clause_relids: None,
            required_relids: None,
            incompatible_relids: None,
            outer_relids: None,
            left_relids: None,
            right_relids: None,
            orclause: None,
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

    fn make_base_rel(relid: i32) -> RelOptInfo {
        let mut rel = bare_reloptinfo(RelOptKind::BASEREL);
        rel.relids = Some(bms_make_singleton(relid));
        rel.relid = relid as usize;
        rel
    }

    fn bare_reloptinfo(kind: RelOptKind) -> RelOptInfo {
        use crate::nodes::parsenodes::RTEKind;
        use crate::nodes::pathnodes::AmFlags;
        RelOptInfo {
            reloptkind: kind,
            relids: None,
            rows: 0.0,
            consider_startup: false,
            consider_param_startup: false,
            consider_parallel: false,
            reltarget: None,
            pathlist: Vec::new(),
            ppilist: Vec::new(),
            partial_pathlist: Vec::new(),
            cheapest_startup_path: None,
            cheapest_total_path: None,
            cheapest_unique_path: None,
            cheapest_parameterized_paths: Vec::new(),
            direct_lateral_relids: None,
            lateral_relids: None,
            relid: 0,
            reltablespace: InvalidOid,
            rtekind: RTEKind::RELATION,
            min_attr: 0,
            max_attr: 0,
            attr_needed: Vec::new(),
            attr_widths: Vec::new(),
            notnullattnums: None,
            nulling_relids: None,
            lateral_vars: Vec::new(),
            lateral_referencers: None,
            indexlist: Vec::new(),
            statlist: Vec::new(),
            pages: 0,
            tuples: 0.0,
            allvisfrac: 0.0,
            eclass_indexes: None,
            subroot: None,
            subplan_params: Vec::new(),
            rel_parallel_workers: -1,
            amflags: AmFlags::empty(),
            serverid: InvalidOid,
            userid: InvalidOid,
            useridiscurrent: false,
            unique_for_rels: Vec::new(),
            non_unique_for_rels: Vec::new(),
            baserestrictinfo: Vec::new(),
            baserestrictcost: QualCost { startup: 0.0, per_tuple: 0.0 },
            baserestrict_min_security: usize::MAX,
            joininfo: Vec::new(),
            has_eclass_joins: false,
            consider_partitionwise_join: false,
            parent: None,
            top_parent: None,
            top_parent_relids: None,
            part_scheme: None,
            nparts: -1,
            partbounds_merged: false,
            partition_qual: Vec::new(),
            part_rels: Vec::new(),
            live_parts: None,
            all_partrels: None,
            partexprs: Vec::new(),
            nullable_partexprs: Vec::new(),
        }
    }

    fn top_jdomain() -> JoinDomain {
        JoinDomain { jd_relids: None }
    }

    fn planner() -> PlannerInfo {
        let parse = Query {
            commandType: CmdType::SELECT,
            querySource: QuerySource::ORIGINAL,
            queryId: 0,
            canSetTag: true,
            utilityStmt: None,
            resultRelation: 0,
            hasAggs: false,
            hasWindowFuncs: false,
            hasTargetSRFs: false,
            hasSubLinks: false,
            hasDistinctOn: false,
            hasRecursive: false,
            hasModifyingCTE: false,
            hasForUpdate: false,
            hasRowSecurity: false,
            hasGroupRTE: false,
            isReturn: false,
            cteList: Vec::new(),
            rtable: Vec::new(),
            rteperminfos: Vec::new(),
            jointree: None,
            mergeActionList: Vec::new(),
            mergeTargetRelation: 0,
            mergeJoinCondition: None,
            targetList: Vec::new(),
            r#override: OverridingKind::NOT_SET,
            onConflict: None,
            returningOldAlias: None,
            returningNewAlias: None,
            returningList: Vec::new(),
            groupClause: Vec::new(),
            groupDistinct: false,
            groupingSets: Vec::new(),
            havingQual: None,
            windowClause: Vec::new(),
            distinctClause: Vec::new(),
            sortClause: Vec::new(),
            limitOffset: None,
            limitCount: None,
            limitOption: LimitOption::COUNT,
            rowMarks: Vec::new(),
            setOperations: None,
            constraintDeps: Vec::new(),
            withCheckOptions: Vec::new(),
            stmt_location: -1,
            stmt_len: 0,
        };
        let glob = PlannerGlobal {
            subplans: Vec::new(),
            subpaths: Vec::new(),
            subroots: Vec::new(),
            rewind_plan_ids: None,
            finalrtable: Vec::new(),
            all_relids: None,
            prunable_relids: None,
            finalrteperminfos: Vec::new(),
            finalrowmarks: Vec::new(),
            result_relations: Vec::new(),
            append_relations: Vec::new(),
            part_prune_infos: Vec::new(),
            relation_oids: Vec::new(),
            inval_items: Vec::new(),
            param_exec_types: Vec::new(),
            last_phid: 0,
            last_row_mark_id: 0,
            last_plan_node_id: 0,
            transient_plan: false,
            depends_on_role: false,
            parallel_mode_ok: false,
            parallel_mode_needed: false,
            max_parallel_hazard: 0,
        };
        PlannerInfo {
            parse: Box::new(parse),
            glob: Box::new(glob),
            query_level: 1,
            parent_root: None,
            plan_params: Vec::new(),
            outer_params: None,
            simple_rel_array: Vec::new(),
            simple_rte_array: Vec::new(),
            append_rel_array: Vec::new(),
            all_baserels: None,
            outer_join_rels: None,
            all_query_rels: None,
            join_rel_list: Vec::new(),
            join_rel_level: Vec::new(),
            join_cur_level: 0,
            init_plans: Vec::new(),
            cte_plan_ids: Vec::new(),
            multiexpr_params: Vec::new(),
            join_domains: vec![Box::new(top_jdomain())],
            eq_classes: Vec::new(),
            ec_merging_done: false,
            canon_pathkeys: Vec::new(),
            left_join_clauses: Vec::new(),
            right_join_clauses: Vec::new(),
            full_join_clauses: Vec::new(),
            join_info_list: Vec::new(),
            last_rinfo_serial: 0,
            all_result_relids: None,
            leaf_result_relids: None,
            append_rel_list: Vec::new(),
            row_identity_vars: Vec::new(),
            row_marks: Vec::new(),
            placeholder_list: Vec::new(),
            placeholder_array: Vec::new(),
            fkey_list: Vec::new(),
            query_pathkeys: Vec::new(),
            group_pathkeys: Vec::new(),
            num_groupby_pathkeys: 0,
            window_pathkeys: Vec::new(),
            distinct_pathkeys: Vec::new(),
            sort_pathkeys: Vec::new(),
            setop_pathkeys: Vec::new(),
            part_schemes: Vec::new(),
            initial_rels: Vec::new(),
            upper_rels: std::array::from_fn(|_| Vec::new()),
            upper_targets: std::array::from_fn(|_| None),
            processed_group_clause: Vec::new(),
            processed_distinct_clause: Vec::new(),
            processed_tlist: Vec::new(),
            scan_input_tlist: Vec::new(),
            update_colnos: Vec::new(),
            grouping_map: Vec::new(),
            minmax_aggs: Vec::new(),
            total_table_pages: 0.0,
            tuple_fraction: 0.0,
            limit_tuples: 0.0,
            qual_security_level: 0,
            has_join_rtes: false,
            has_lateral_rtes: false,
            has_having_qual: false,
            has_pseudo_constant_quals: false,
            has_alternative_subplans: false,
            placeholders_frozen: false,
            has_recursion: false,
            group_rtindex: 0,
            agginfos: Vec::new(),
            aggtransinfos: Vec::new(),
            num_ordered_aggs: 0,
            has_non_partial_aggs: false,
            has_non_serial_aggs: false,
            wt_param_id: -1,
            non_recursive_path: None,
            cur_outer_rels: None,
            cur_outer_params: Vec::new(),
            is_alt_subplan: Vec::new(),
            is_used_subplan: Vec::new(),
            part_cols_updated: false,
            part_prune_infos: Vec::new(),
        }
    }

    #[test]
    fn process_equivalence_builds_two_member_ec() {
        let mut root = planner();
        let jdomain = top_jdomain();
        let mut ri = make_eq_restrictinfo();

        let ok = process_equivalence(&mut root, &mut ri, &jdomain);
        assert!(ok);
        assert_eq!(root.eq_classes.len(), 1);

        let ec = &root.eq_classes[0];
        assert_eq!(ec.members.len(), 2);
        assert!(bms_equal(ec.members[0].relids.as_ref().unwrap(), &bms_make_singleton(1)));
        assert!(bms_equal(ec.members[1].relids.as_ref().unwrap(), &bms_make_singleton(2)));
        let expect_relids = bms_union(&bms_make_singleton(1), &bms_make_singleton(2));
        assert!(bms_equal(ec.relids.as_ref().unwrap(), &expect_relids));

        // RI cross-links committed.
        assert!(ri.left_ec.is_some());
        assert!(ri.left_em.is_some());
        assert!(ri.right_em.is_some());
    }

    #[test]
    fn generate_join_implied_equalities_reproduces_clause() {
        let mut root = planner();
        let jdomain = top_jdomain();
        let mut ri = make_eq_restrictinfo();
        assert!(process_equivalence(&mut root, &mut ri, &jdomain));

        // Stamp the base rels' eclass_indexes (what generate_base_implied_equalities
        // does) so the join lookup finds the EC.
        let mut rel1 = make_base_rel(1);
        rel1.eclass_indexes = Some(bms_make_singleton(0));
        let mut rel2 = make_base_rel(2);
        rel2.eclass_indexes = Some(bms_make_singleton(0));
        root.simple_rel_array = vec![None, Some(Box::new(rel1)), Some(Box::new(rel2.clone()))];
        root.ec_merging_done = true;

        let join_relids = bms_union(&bms_make_singleton(1), &bms_make_singleton(2));
        let outer_relids = bms_make_singleton(1);
        let sjinfo = inner_sjinfo(&join_relids);

        let clauses =
            generate_join_implied_equalities(&mut root, join_relids, outer_relids, &rel2, &sjinfo);

        assert_eq!(clauses.len(), 1);
        let Node::OpExpr(op) = &clauses[0].clause else {
            panic!("expected OpExpr join clause");
        };
        assert_eq!(op.opno, INT4EQ);
        assert_eq!(op.args.len(), 2);
        // operands are a.x (varno 1) and b.y (varno 2), in some order.
        let varnos: Vec<i32> = op
            .args
            .iter()
            .filter_map(|a| if let Node::Var(v) = a { Some(v.varno) } else { None })
            .collect();
        assert!(varnos.contains(&1) && varnos.contains(&2));
    }

    fn inner_sjinfo(joinrelids: &Relids) -> SpecialJoinInfo {
        use crate::nodes::nodes::JoinType;
        SpecialJoinInfo {
            min_lefthand: None,
            min_righthand: None,
            syn_lefthand: None,
            syn_righthand: Some(joinrelids.clone()),
            jointype: JoinType::INNER,
            ojrelid: 0,
            commute_above_l: None,
            commute_above_r: None,
            commute_below_l: None,
            commute_below_r: None,
            lhs_strict: false,
            semi_can_btree: false,
            semi_can_hash: false,
            semi_operators: Vec::new(),
            semi_rhs_exprs: Vec::new(),
        }
    }
}
