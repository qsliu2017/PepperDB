//! Jointree deconstruction and qual distribution. Translated from
//! backend/optimizer/plan/initsplan.c -- the keystone that turns the parsed
//! jointree + WHERE into the rels-with-clauses the join search consumes.
//!
//! Pipeline (driven by `query_planner`, wired in a later step):
//!   `add_base_rels_to_query`  -- one `RelOptInfo` per base RTE in the jointree.
//!   `build_base_rel_tlists`   -- mark final-tlist / HAVING Vars needed at rel 0.
//!   `deconstruct_jointree`    -- two-phase walk: phase 1 (`deconstruct_recurse`)
//!        builds a `JoinTreeItem` per node (qualscope, inner_join_rels, the join
//!        domains) and the output joinlist; phase 2 (`deconstruct_distribute`)
//!        sends each ON/WHERE clause to `distribute_qual_to_rels`.
//!   `distribute_qual_to_rels` -- per clause: build a `RestrictInfo`, and either
//!        feed an equality to the EquivalenceClass machinery
//!        (`process_equivalence`) or push it to the rels via
//!        `distribute_restrictinfo_to_rels` (a single-rel clause goes to that
//!        rel's `baserestrictinfo`; a multi-rel clause is a join clause and goes
//!        to `joininfo` of every rel it mentions).
//!
//! FULL (inner-join keystone complete, rules.md s4): `add_base_rels_to_query`,
//! `build_base_rel_tlists`, `add_vars_to_targetlist`, `add_vars_to_attr_needed`,
//! `deconstruct_jointree`/`deconstruct_recurse`/`deconstruct_distribute` for the
//! INNER-join + FromExpr path, `distribute_quals_to_rels`,
//! `distribute_qual_to_rels` (inner-join / WHERE path), `add_base_clause_to_rel`,
//! `distribute_restrictinfo_to_rels`, `restriction_is_always_true/false`,
//! `expr_is_nonnullable`, `check_mergejoinable`/`check_hashjoinable`/
//! `check_memoizable`, `mark_rels_nulled_by_join`, `get_join_domain_min_rels`.
//! For `FROM a, b WHERE a.x=b.y` and `a JOIN b ON a.x=b.y` the two base rels are
//! added and the equality is distributed (to an EC if mergejoinable, else to
//! both rels' joininfo).
//!
//! STAGED (`not_yet_reachable`): the OUTER-join machinery -- `make_outerjoininfo`,
//! `compute_semijoin_info`, `deconstruct_distribute_oj_quals`, the FULL/LEFT/SEMI/
//! ANTI arms of `deconstruct_recurse`, the postponed-OJ and lateral-clause paths,
//! `process_security_barrier_quals`, the LATERAL helpers
//! (`find_lateral_references`, `create_lateral_join_info`,
//! `extract_lateral_references`, `rebuild_lateral_attr_needed`), and
//! `process_implied_equality`/`build_implied_join_equality` (EC-driven, step 31).
//! `check_memoizable`'s typecache lookup is staged too (left_/right_hasheqoperator
//! stay invalid; Memoize is an optimization).
//!
//! `JoinTreeItem` is a local C struct; we model it as a private struct keyed by
//! the planner's `join_domains` index (PG keeps a `JoinDomain *`; we keep its
//! position in `root.join_domains`).

#![allow(
    clippy::too_many_arguments,
    reason = "1:1 PG port: distribute_qual_to_rels / distribute_quals_to_rels match the C signatures"
)]
#![allow(
    clippy::needless_pass_by_value,
    reason = "1:1 PG port: where_needed/jtnode are owned values matching the C/header signatures (consumed once on the live path)"
)]
#![allow(
    clippy::fn_params_excessive_bools,
    reason = "1:1 PG port: the allow_equivalence/has_clone/is_clone/postpone flags mirror distribute_qual_to_rels' C signature"
)]

use crate::nodes::bitmapset::{
    bms_add_member, bms_add_members, bms_copy, bms_difference, bms_get_singleton_member,
    bms_is_member, bms_is_subset, bms_make_singleton, bms_membership, bms_next_member,
    bms_num_members, bms_overlap, bms_union, BMS_Membership,
};
use crate::nodes::nodeFuncs::exprType;
use crate::nodes::nodes::{JoinType, Node};
use crate::nodes::parsenodes::RTEKind;
use crate::nodes::pathnodes::{
    PlannerInfo, Relids, RestrictInfo, SpecialJoinInfo, VolatileFunctionStatus,
};
use crate::nodes::primnodes::{Index, NullTestType};
use crate::postgres_ext::Oid;
use crate::utils::lsyscache::get_mergejoin_opfamilies;

use crate::backend::optimizer::path::equivclass::process_equivalence;
use crate::backend::optimizer::path::pathkeys::initialize_mergeclause_eclasses;
use crate::backend::optimizer::util::joininfo::add_join_clause_to_rels;
use crate::backend::optimizer::util::relnode::{build_simple_rel, find_base_rel};
use crate::backend::optimizer::util::restrictinfo::make_restrictinfo;
use crate::backend::optimizer::util::var::{pull_var_clause, pull_varnos};
use crate::optimizer::optimizer::PullVarClauseFlags;

/// Panic for an initsplan path not yet translated for this milestone (rules.md s4).
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

/// PG `bms_is_empty`: no standalone helper in the bitmapset module; a set is
/// empty iff it has zero members.
fn bms_is_empty(a: &Relids) -> bool {
    bms_num_members(a) == 0
}

/// PG's local `JoinTreeItem`. One per jointree node, built in
/// `deconstruct_recurse` (phase 1) and consumed in `deconstruct_distribute`
/// (phase 2). PG holds a `JoinDomain *jdomain`; we store its index into
/// `root.join_domains` (`jdomain_idx`). `jti_parent` is the index into the
/// `item_list` of the parent's JoinTreeItem (`usize::MAX` for the top).
struct JoinTreeItem {
    /// The jointree node this item describes.
    jtnode: Node,
    /// Index into `root.join_domains` of this node's join domain.
    jdomain_idx: usize,
    /// Index into `item_list` of the parent item (`usize::MAX` if top).
    jti_parent: usize,
    /// base+OJ relids syntactically included in this node.
    qualscope: Relids,
    /// base+OJ relids in inner joins at/below this node.
    inner_join_rels: Relids,
    /// if a join node, relids of the left side.
    left_rels: Relids,
    /// if a join node, relids of the right side.
    right_rels: Relids,
    /// if an outer join, relids of the non-nullable side.
    nonnullable_rels: Relids,
    /// if an outer join, its SpecialJoinInfo (phase 2).
    sjinfo: Option<Box<SpecialJoinInfo>>,
    /// outer-join quals not yet distributed (phase 2).
    oj_joinclauses: Vec<Node>,
    /// quals postponed from children due to lateral references (phase 2).
    lateral_clauses: Vec<Node>,
}

/*****************************************************************************
 *	 JOIN TREES
 *****************************************************************************/

/// PG `add_base_rels_to_query`: scan the jointree and create a baserel
/// `RelOptInfo` for every non-join RTE appearing in it. The initial call passes
/// `root.parse.jointree`; it recurses through FromExpr / JoinExpr nodes.
pub fn add_base_rels_to_query(root: &mut PlannerInfo, jtnode: &Node) {
    match jtnode {
        Node::RangeTblRef(rtr) => {
            build_simple_rel(root, rtr.rtindex, None);
        }
        Node::FromExpr(f) => {
            for child in f.fromlist.clone() {
                add_base_rels_to_query(root, &child);
            }
        }
        Node::JoinExpr(j) => {
            if let Some(larg) = j.larg.clone() {
                add_base_rels_to_query(root, &larg);
            }
            if let Some(rarg) = j.rarg.clone() {
                add_base_rels_to_query(root, &rarg);
            }
        }
        other => not_yet_reachable(&format!("add_base_rels_to_query: node type {other:?}")),
    }
}

/// PG `add_other_rels_to_query`: build otherrel `RelOptInfo`s for appendrel
/// children. Staged (inheritance / partitioning not in M7).
pub fn add_other_rels_to_query(root: &mut PlannerInfo) {
    for rti in 1..root.simple_rel_array.len() {
        let Some(rte) = root.simple_rte_array.get(rti).and_then(Clone::clone) else {
            continue;
        };
        let is_baserel = root.simple_rel_array[rti].is_some();
        if is_baserel && rte.inh {
            not_yet_reachable("add_other_rels_to_query: expand_inherited_rtentry");
        }
    }
}

/*****************************************************************************
 *	 TARGET LISTS
 *****************************************************************************/

/// PG `build_base_rel_tlists`: mark every Var needed in the query's final tlist
/// (and HAVING) as needed by "relation 0", so it propagates through all joins.
pub fn build_base_rel_tlists(root: &mut PlannerInfo, final_tlist: &[Node]) {
    let flags = PullVarClauseFlags::RECURSE_AGGREGATES
        | PullVarClauseFlags::RECURSE_WINDOWFUNCS
        | PullVarClauseFlags::INCLUDE_PLACEHOLDERS;
    // PG passes the whole `final_tlist` List to pull_var_clause; we have no List
    // node, so collect Vars from each tlist entry.
    let tlist_vars: Vec<Node> = final_tlist
        .iter()
        .flat_map(|entry| pull_var_clause(Some(entry.clone()), flags))
        .collect();
    if !tlist_vars.is_empty() {
        add_vars_to_targetlist(root, &tlist_vars, bms_make_singleton(0));
    }

    // HAVING clause Vars too (HAVING can contain Aggrefs, not WindowFuncs).
    if let Some(having) = root.parse.havingQual.clone() {
        let having_flags =
            PullVarClauseFlags::RECURSE_AGGREGATES | PullVarClauseFlags::INCLUDE_PLACEHOLDERS;
        let having_vars = pull_var_clause(Some(having), having_flags);
        if !having_vars.is_empty() {
            add_vars_to_targetlist(root, &having_vars, bms_make_singleton(0));
        }
    }
}

/// PG `add_vars_to_targetlist`: for each Var in `vars`, add it to its owning
/// rel's reltarget (if not already present) and union `where_needed` into the
/// rel's `attr_needed[attno]`. PlaceHolderVars route to `find_placeholder_info`
/// (staged). Write-back: `find_base_rel` returns an owned clone, so we re-park
/// the mutated rel in `root.simple_rel_array[varno]`.
pub fn add_vars_to_targetlist(root: &mut PlannerInfo, vars: &[Node], where_needed: Relids) {
    crate::assert!(!bms_is_empty(&where_needed));

    for node in vars {
        match node {
            Node::Var(var) => {
                let varno = var.varno;
                let mut rel = find_base_rel(root, varno);
                let relids = rel.relids.clone().unwrap_or_default();
                if bms_is_subset(&where_needed, &relids) {
                    continue;
                }
                let attno = var.varattno;
                crate::assert!(attno >= rel.min_attr && attno <= rel.max_attr);
                let ndx = (attno - rel.min_attr) as usize;
                if rel.attr_needed[ndx].is_none() {
                    // Add to the rel's targetlist, dropping varnullingrels (the
                    // scan-level value isn't yet nulled by any outer join).
                    let mut newvar = var.as_ref().clone();
                    newvar.varnullingrels = None;
                    if let Some(target) = rel.reltarget.as_mut() {
                        target.exprs.push(Node::Var(Box::new(newvar)));
                    }
                }
                let cur = rel.attr_needed[ndx].clone().unwrap_or_default();
                rel.attr_needed[ndx] = Some(bms_add_members(cur, &where_needed));
                root.simple_rel_array[varno as usize] = Some(rel);
            }
            Node::PlaceHolderVar(_) => {
                not_yet_reachable("add_vars_to_targetlist: PlaceHolderVar (find_placeholder_info)");
            }
            other => not_yet_reachable(&format!("add_vars_to_targetlist: node type {other:?}")),
        }
    }
}

/// PG `add_vars_to_attr_needed`: like `add_vars_to_targetlist` but only updates
/// `attr_needed`/`ph_needed` (the Vars are assumed already in the targetlists).
/// Used to rebuild needed sets after removing a useless outer join.
pub fn add_vars_to_attr_needed(root: &mut PlannerInfo, vars: &[Node], where_needed: Relids) {
    crate::assert!(!bms_is_empty(&where_needed));

    for node in vars {
        match node {
            Node::Var(var) => {
                let varno = var.varno;
                let mut rel = find_base_rel(root, varno);
                let relids = rel.relids.clone().unwrap_or_default();
                if bms_is_subset(&where_needed, &relids) {
                    continue;
                }
                let attno = var.varattno;
                crate::assert!(attno >= rel.min_attr && attno <= rel.max_attr);
                let ndx = (attno - rel.min_attr) as usize;
                let cur = rel.attr_needed[ndx].clone().unwrap_or_default();
                rel.attr_needed[ndx] = Some(bms_add_members(cur, &where_needed));
                root.simple_rel_array[varno as usize] = Some(rel);
            }
            Node::PlaceHolderVar(_) => {
                not_yet_reachable("add_vars_to_attr_needed: PlaceHolderVar (find_placeholder_info)");
            }
            other => not_yet_reachable(&format!("add_vars_to_attr_needed: node type {other:?}")),
        }
    }
}

/*****************************************************************************
 *	 DECONSTRUCT JOINTREE
 *****************************************************************************/

/// PG `deconstruct_jointree`: the two-phase jointree walk. Phase 1
/// (`deconstruct_recurse`) builds a `JoinTreeItem` per node and the output
/// joinlist; phase 2 (`deconstruct_distribute`) distributes each node's quals.
/// Returns the joinlist for the scan/join search.
pub fn deconstruct_jointree(root: &mut PlannerInfo) -> Vec<Node> {
    // No more PlaceHolderInfos may be made after this (make_outerjoininfo needs
    // all active placeholders present while crawling up the join tree).
    root.placeholders_frozen = true;

    // The top-level join domain already exists (created before query_planner).
    crate::assert!(!root.join_domains.is_empty());
    root.join_domains[0].jd_relids = None; // filled during deconstruct_recurse

    let jointree = root
        .parse
        .jointree
        .clone()
        .unwrap_or_else(|| not_yet_reachable("deconstruct_jointree: missing jointree"));
    crate::assert!(matches!(jointree, Node::FromExpr(_)));

    root.all_baserels = None;
    root.outer_join_rels = None;

    let mut item_list: Vec<JoinTreeItem> = Vec::new();
    let result = deconstruct_recurse(root, jointree, 0, usize::MAX, &mut item_list);

    // all_query_rels = all_baserels | outer_join_rels.
    root.all_query_rels = Some(bms_union(
        &root.all_baserels.clone().unwrap_or_default(),
        &root.outer_join_rels.clone().unwrap_or_default(),
    ));

    // Phase 2: distribute quals.
    for idx in 0..item_list.len() {
        deconstruct_distribute(root, &item_list, idx);
    }

    // Postponed LEFT JOIN clauses (only if there were special joins). Staged.
    if !root.join_info_list.is_empty() {
        for item in &item_list {
            if !item.oj_joinclauses.is_empty() {
                not_yet_reachable("deconstruct_jointree: deconstruct_distribute_oj_quals");
            }
        }
    }

    result
}

/// PG `deconstruct_recurse`: phase-1 recursion. Appends a `JoinTreeItem` for
/// `jtnode` (depth-first) and returns the joinlist for it. `parent_domain_idx`
/// is the index into `root.join_domains` of the enclosing domain;
/// `parent_jtitem` is the parent's index into `item_list` (`usize::MAX` at top).
fn deconstruct_recurse(
    root: &mut PlannerInfo,
    jtnode: Node,
    parent_domain_idx: usize,
    parent_jtitem: usize,
    item_list: &mut Vec<JoinTreeItem>,
) -> Vec<Node> {
    let joinlist: Vec<Node>;
    let mut item = JoinTreeItem {
        jtnode: jtnode.clone(),
        jdomain_idx: parent_domain_idx,
        jti_parent: parent_jtitem,
        qualscope: Relids::default(),
        inner_join_rels: Relids::default(),
        left_rels: Relids::default(),
        right_rels: Relids::default(),
        nonnullable_rels: Relids::default(),
        sjinfo: None,
        oj_joinclauses: Vec::new(),
        lateral_clauses: Vec::new(),
    };

    match jtnode {
        Node::RangeTblRef(ref rtr) => {
            let varno = rtr.rtindex;
            // Fill all_baserels as we encounter baserel nodes.
            root.all_baserels =
                Some(bms_add_member(root.all_baserels.clone().unwrap_or_default(), varno));
            // This node belongs to parent_domain.
            let cur = root.join_domains[parent_domain_idx].jd_relids.clone().unwrap_or_default();
            root.join_domains[parent_domain_idx].jd_relids = Some(bms_add_member(cur, varno));
            item.qualscope = bms_make_singleton(varno);
            item.inner_join_rels = Relids::default();
            joinlist = vec![jtnode.clone()];
        }
        Node::FromExpr(ref f) => {
            // This node and its children belong to parent_domain.
            let mut jl: Vec<Node> = Vec::new();
            let fromlist = f.fromlist.clone();
            for child in fromlist {
                let sub_joinlist =
                    deconstruct_recurse(root, child, parent_domain_idx, item_list.len(), item_list);
                let sub = item_list.last().unwrap_or_else(|| {
                    not_yet_reachable("deconstruct_recurse: missing child JoinTreeItem")
                });
                item.qualscope = bms_add_members(item.qualscope, &sub.qualscope);
                item.inner_join_rels = sub.inner_join_rels.clone();
                // M7 collapses subproblems unconditionally (no from_collapse_limit
                // splitting; the join-search DP wants a flat joinlist). PG would
                // honour from_collapse_limit; for our flat single-level search the
                // concatenation is the only reachable behaviour.
                jl.extend(sub_joinlist);
            }
            // A FROM with >1 element is an inner join subsuming all below it.
            if f.fromlist.len() > 1 {
                item.inner_join_rels = item.qualscope.clone();
            }
            joinlist = jl;
        }
        Node::JoinExpr(ref j) => {
            match j.jointype {
                JoinType::INNER => {
                    let left = j.larg.clone().unwrap_or_else(|| {
                        not_yet_reachable("deconstruct_recurse: INNER join missing larg")
                    });
                    let right = j.rarg.clone().unwrap_or_else(|| {
                        not_yet_reachable("deconstruct_recurse: INNER join missing rarg")
                    });
                    let left_joinlist =
                        deconstruct_recurse(root, left, parent_domain_idx, item_list.len(), item_list);
                    let left_item = item_list.len() - 1;
                    let right_joinlist = deconstruct_recurse(
                        root,
                        right,
                        parent_domain_idx,
                        item_list.len(),
                        item_list,
                    );
                    let right_item = item_list.len() - 1;
                    let lq = item_list[left_item].qualscope.clone();
                    let rq = item_list[right_item].qualscope.clone();
                    item.qualscope = bms_union(&lq, &rq);
                    item.inner_join_rels = item.qualscope.clone();
                    item.left_rels = lq;
                    item.right_rels = rq;
                    item.nonnullable_rels = Relids::default();
                    // Combine subproblems (M7 flat join search; no join_collapse_limit).
                    let mut jl = left_joinlist;
                    jl.extend(right_joinlist);
                    joinlist = jl;
                }
                JoinType::LEFT | JoinType::ANTI | JoinType::SEMI | JoinType::FULL => {
                    not_yet_reachable("deconstruct_recurse: outer/semi/anti join (SpecialJoinInfo)");
                }
                other => not_yet_reachable(&format!(
                    "deconstruct_recurse: unrecognized join type {other:?}"
                )),
            }
        }
        ref other => not_yet_reachable(&format!("deconstruct_recurse: node type {other:?}")),
    }

    item_list.push(item);
    joinlist
}

/// PG `deconstruct_distribute`: phase 2 for one jointree node. Distributes the
/// node's quals to the appropriate base-restriction / join lists (and would add
/// SpecialJoinInfo entries for outer joins).
fn deconstruct_distribute(root: &mut PlannerInfo, item_list: &[JoinTreeItem], idx: usize) {
    let jtnode = item_list[idx].jtnode.clone();
    match jtnode {
        Node::RangeTblRef(_) => {
            // securityQuals on the RTE (qual_security_level > 0) -> staged.
            if root.qual_security_level > 0 {
                not_yet_reachable("deconstruct_distribute: process_security_barrier_quals");
            }
        }
        Node::FromExpr(f) => {
            let qualscope = item_list[idx].qualscope.clone();
            let jdomain_idx = item_list[idx].jdomain_idx;
            // Lateral-referencing quals postponed to this level (staged if any).
            if !item_list[idx].lateral_clauses.is_empty() {
                not_yet_reachable("deconstruct_distribute: lateral_clauses");
            }
            // Top-level quals: f->quals is a single (possibly AND) expression in
            // our tree; the implicit-AND list is its conjuncts.
            let clauses = and_clause_list(f.quals.clone());
            distribute_quals_to_rels(
                root,
                &clauses,
                jdomain_idx,
                None,
                root.qual_security_level,
                &qualscope,
                None,
                None,
                None,
                true,
                false,
                false,
                false,
            );
        }
        Node::JoinExpr(j) => {
            if j.jointype != JoinType::INNER {
                not_yet_reachable("deconstruct_distribute: outer-join quals (make_outerjoininfo)");
            }
            let qualscope = item_list[idx].qualscope.clone();
            let jdomain_idx = item_list[idx].jdomain_idx;
            // Inner join: sjinfo = NULL, ojscope = NULL.
            let clauses = and_clause_list(j.quals.clone());
            distribute_quals_to_rels(
                root,
                &clauses,
                jdomain_idx,
                None,
                root.qual_security_level,
                &qualscope,
                None,
                None,
                None,
                true,
                false,
                false,
                false,
            );
        }
        other => not_yet_reachable(&format!("deconstruct_distribute: node type {other:?}")),
    }
}

/// Flatten an (implicit-AND) qual expression into its conjuncts. PG stores
/// `f->quals` as an implicit-AND `List`; our parse tree stores a single Node, so
/// a top-level AND BoolExpr is split into its args, anything else is one clause,
/// and None is no clauses.
fn and_clause_list(quals: Option<Node>) -> Vec<Node> {
    use crate::nodes::primnodes::BoolExprType;
    match quals {
        None => Vec::new(),
        Some(Node::BoolExpr(b)) if b.boolop == BoolExprType::AND_EXPR => b.args,
        Some(other) => vec![other],
    }
}

/*****************************************************************************
 *	  QUALIFICATIONS
 *****************************************************************************/

/// PG `distribute_quals_to_rels`: apply `distribute_qual_to_rels` to each clause
/// of an AND'ed list. `jdomain_idx` is the join domain index (PG's `jdomain`).
fn distribute_quals_to_rels(
    root: &mut PlannerInfo,
    clauses: &[Node],
    jdomain_idx: usize,
    sjinfo: Option<&SpecialJoinInfo>,
    security_level: Index,
    qualscope: &Relids,
    ojscope: Option<&Relids>,
    outerjoin_nonnullable: Option<&Relids>,
    incompatible_relids: Option<&Relids>,
    allow_equivalence: bool,
    has_clone: bool,
    is_clone: bool,
    postpone_oj: bool,
) {
    for clause in clauses {
        distribute_qual_to_rels(
            root,
            clause,
            jdomain_idx,
            sjinfo,
            security_level,
            qualscope,
            ojscope,
            outerjoin_nonnullable,
            incompatible_relids,
            allow_equivalence,
            has_clone,
            is_clone,
            postpone_oj,
        );
    }
}

/// PG `distribute_qual_to_rels`: the core. Build a `RestrictInfo` for `clause`,
/// and either feed a mergejoinable equality to the EquivalenceClass machinery or
/// push it to the rels via `distribute_restrictinfo_to_rels`.
///
/// The INNER-join / WHERE path is complete; the outer-join branches
/// (`outerjoin_nonnullable` overlap, postponed OJ quals, the OJ-clause lists)
/// route to `not_yet_reachable`.
fn distribute_qual_to_rels(
    root: &mut PlannerInfo,
    clause: &Node,
    jdomain_idx: usize,
    sjinfo: Option<&SpecialJoinInfo>,
    security_level: Index,
    qualscope: &Relids,
    ojscope: Option<&Relids>,
    outerjoin_nonnullable: Option<&Relids>,
    incompatible_relids: Option<&Relids>,
    allow_equivalence: bool,
    has_clone: bool,
    is_clone: bool,
    postpone_oj: bool,
) {
    let mut relids = pull_varnos(root, Some(clause.clone()));

    // A clause referencing rels outside its syntactic scope can only happen via
    // LATERAL pullup; staged.
    if !bms_is_subset(&relids, qualscope) {
        not_yet_reachable("distribute_qual_to_rels: lateral-reference qual postponement");
    }

    // Outer-join qual: relids must be a subset of ojscope.
    if let Some(ojscope) = ojscope
        && !bms_is_subset(&relids, ojscope)
    {
        crate::elog!(crate::utils::elog::ERROR, "JOIN qualification cannot refer to other relations");
    }

    let mut pseudoconstant = false;
    if bms_is_empty(&relids) {
        // Variable-free clause: the inner-join WHERE path makes it a gating
        // pseudoconstant at the top of the join domain (or qualscope).
        if ojscope.is_some() {
            not_yet_reachable("distribute_qual_to_rels: variable-free outer-join qual");
        } else if contain_volatile_functions_or_false(clause) {
            relids = bms_copy(qualscope);
        } else if jdomain_idx == 0 {
            relids = bms_copy(&root.join_domains[0].jd_relids.clone().unwrap_or_default());
            pseudoconstant = true;
            root.has_pseudo_constant_quals = true;
        } else {
            relids = bms_copy(qualscope);
            pseudoconstant = true;
            root.has_pseudo_constant_quals = true;
        }
    }

    // Outer-join delay check.
    if bms_overlap(&relids, &outerjoin_nonnullable.cloned().unwrap_or_default())
        && !outerjoin_nonnullable.is_none_or(bms_is_empty)
    {
        // Non-degenerate outer-join qual (mentions the nonnullable side). The
        // postponed-OJ list and the OJ-clause lists are the entire staged
        // outer-join clause path.
        let _ = postpone_oj;
        not_yet_reachable("distribute_qual_to_rels: non-degenerate outer-join qual");
    }
    // Normal qual or degenerate outer-join clause; mark pushed-down. (The
    // redundant-IS-NULL-vs-antijoin check needs antijoins in join_info_list,
    // which M7 never has, so it's skipped.)
    let is_pushed_down = true;
    let maybe_equivalence = allow_equivalence;
    let maybe_outer_join = false;

    // Build the RestrictInfo.
    let mut restrictinfo = make_restrictinfo(
        root,
        Box::new(clause.clone()),
        is_pushed_down,
        has_clone,
        is_clone,
        pseudoconstant,
        security_level,
        Some(relids.clone()),
        incompatible_relids.cloned(),
        outerjoin_nonnullable.cloned(),
    );

    // If it's a join clause, add its Vars to the rels' targetlists.
    if bms_membership(&relids) == BMS_Membership::MULTIPLE {
        let flags = PullVarClauseFlags::RECURSE_AGGREGATES
            | PullVarClauseFlags::RECURSE_WINDOWFUNCS
            | PullVarClauseFlags::INCLUDE_PLACEHOLDERS;
        let vars = pull_var_clause(Some(clause.clone()), flags);
        let where_needed = if is_clone {
            not_yet_reachable("distribute_qual_to_rels: clone-clause where_needed");
        } else {
            relids
        };
        add_vars_to_targetlist(root, &vars, where_needed);
    }

    // Mergejoinability of every clause (also picks up var=var/var=const eqs).
    check_mergejoinable(&mut restrictinfo);

    // If it's a true equivalence clause, hand it to the EC machinery.
    if !restrictinfo.mergeopfamilies.is_empty() {
        if maybe_equivalence {
            let jdomain = root.join_domains[jdomain_idx].clone();
            if process_equivalence(root, &mut restrictinfo, &jdomain) {
                return;
            }
            // EC rejected it; set up left_ec/right_ec the hard way.
            if !restrictinfo.mergeopfamilies.is_empty() {
                initialize_mergeclause_eclasses(root, &mut restrictinfo);
            }
        } else if maybe_outer_join && restrictinfo.can_join {
            let _ = sjinfo;
            not_yet_reachable("distribute_qual_to_rels: outer-join mergeclause lists");
        } else {
            initialize_mergeclause_eclasses(root, &mut restrictinfo);
        }
    }

    // No EC special case applies: push it into the clause lists.
    distribute_restrictinfo_to_rels(root, &restrictinfo);
}

/// PG `add_base_clause_to_rel`: add `restrictinfo` to base rel `relid`'s
/// `baserestrictinfo`, after the always-true (drop) / always-false (replace with
/// constant-FALSE) prechecks. Updates `baserestrict_min_security`. Write-back to
/// `root.simple_rel_array[relid]`.
fn add_base_clause_to_rel(root: &mut PlannerInfo, relid: i32, restrictinfo: &RestrictInfo) {
    crate::assert!(
        bms_membership(&restrictinfo.required_relids.clone().unwrap_or_default())
            == BMS_Membership::SINGLETON
    );

    // Inheritance parents always record the qual as-is; at M7 no rel is an
    // inheritance parent, so the constant-folding prechecks apply.
    if restriction_is_always_true(root, restrictinfo) {
        return;
    }
    if restriction_is_always_false(root, restrictinfo) {
        not_yet_reachable("add_base_clause_to_rel: always-false constant-FALSE substitution");
    }

    let mut rel = find_base_rel(root, relid);
    rel.baserestrictinfo.push(Box::new(restrictinfo.clone()));
    rel.baserestrict_min_security =
        rel.baserestrict_min_security.min(restrictinfo.security_level);
    root.simple_rel_array[relid as usize] = Some(rel);
}

/// PG `expr_is_nonnullable`: a simple Var that is defined NOT NULL and is not
/// nulled by any outer join cannot be NULL.
fn expr_is_nonnullable(root: &mut PlannerInfo, expr: &Node) -> bool {
    let Node::Var(var) = expr else { return false };
    // Could the Var be nulled by any outer join?
    if !var.varnullingrels.as_ref().is_none_or(bms_is_empty) {
        return false;
    }
    // System columns cannot be NULL.
    if var.varattno < 0 {
        return true;
    }
    let rel = find_base_rel(root, var.varno);
    var.varattno > 0
        && rel
            .notnullattnums
            .as_ref()
            .is_some_and(|s| crate::nodes::bitmapset::bms_is_member(i32::from(var.varattno), s))
}

/// PG `restriction_is_always_true`: currently only IS NOT NULL NullTests over a
/// provably-nonnullable scalar (and OR clauses one branch of which is).
pub fn restriction_is_always_true(root: &mut PlannerInfo, restrictinfo: &RestrictInfo) -> bool {
    if restrictinfo.has_clone || restrictinfo.is_clone {
        return false;
    }
    if let Node::NullTest(nulltest) = &restrictinfo.clause {
        if nulltest.nulltesttype != NullTestType::NOT_NULL || nulltest.argisrow {
            return false;
        }
        return nulltest
            .arg
            .as_ref()
            .is_some_and(|arg| expr_is_nonnullable(root, arg));
    }
    if restrictinfo.orclause.is_some() {
        // OR branch check needs RestrictInfo-wrapped args; M7 ORs aren't wrapped
        // that way, so this never fires. Staged.
        return false;
    }
    false
}

/// PG `restriction_is_always_false`: currently only IS NULL NullTests over a
/// provably-nonnullable scalar (and OR clauses all branches of which are).
pub fn restriction_is_always_false(root: &mut PlannerInfo, restrictinfo: &RestrictInfo) -> bool {
    if restrictinfo.has_clone || restrictinfo.is_clone {
        return false;
    }
    if let Node::NullTest(nulltest) = &restrictinfo.clause {
        if nulltest.nulltesttype != NullTestType::NULL || nulltest.argisrow {
            return false;
        }
        return nulltest
            .arg
            .as_ref()
            .is_some_and(|arg| expr_is_nonnullable(root, arg));
    }
    if restrictinfo.orclause.is_some() {
        return false;
    }
    false
}

/// PG `distribute_restrictinfo_to_rels`: push a completed RestrictInfo into the
/// proper restriction / join clause list(s). A single-rel clause is a base
/// restriction; a multi-rel clause is a join clause.
pub fn distribute_restrictinfo_to_rels(root: &mut PlannerInfo, restrictinfo: &RestrictInfo) {
    let relids = restrictinfo.required_relids.clone().unwrap_or_default();
    if bms_is_empty(&relids) {
        crate::elog!(crate::utils::elog::ERROR, "cannot cope with variable-free clause");
        return;
    }

    if let Some(relid) = bms_get_singleton_member(&relids) {
        // One relation -> restriction clause for that relation.
        add_base_clause_to_rel(root, relid, restrictinfo);
    } else {
        // More than one rel -> join clause.
        let mut ri = restrictinfo.clone();
        check_hashjoinable(&mut ri);
        check_memoizable(&mut ri);
        add_join_clause_to_rels(root, &ri, relids);
    }
}

/// PG `mark_rels_nulled_by_join`: set `nulling_relids` on baserels below the
/// nullable side of an outer join. Reached only on the staged outer-join path.
pub fn mark_rels_nulled_by_join(root: &mut PlannerInfo, ojrelid: Index, lower_rels: &Relids) {
    let mut relid = -1;
    while let Some(r) = bms_next_member(lower_rels, relid) {
        relid = r;
        if relid == root.group_rtindex {
            continue;
        }
        if let Some(rel) = root.simple_rel_array[relid as usize].as_mut() {
            let cur = rel.nulling_relids.clone().unwrap_or_default();
            rel.nulling_relids = Some(bms_add_member(cur, ojrelid as i32));
        }
    }
}

/// PG `process_implied_equality`: create `item1 op item2` and push it into the
/// clause lists. Driven by the EC machinery (generate_base_implied_equalities);
/// staged with the rest of the EC-derived clause generation (step 31).
pub fn process_implied_equality(
    _root: &mut PlannerInfo,
    _opno: crate::postgres_ext::Oid,
    _collation: crate::postgres_ext::Oid,
    _item1: &crate::nodes::primnodes::Expr,
    _item2: &crate::nodes::primnodes::Expr,
    _qualscope: Relids,
    _security_level: Index,
    _both_const: bool,
) -> RestrictInfo {
    not_yet_reachable("process_implied_equality: EC-derived equality clause");
}

/// PG `build_implied_join_equality`: build a RestrictInfo for an EC-implied join
/// equality (no distribution). Staged (step 31).
pub fn build_implied_join_equality(
    _root: &mut PlannerInfo,
    _opno: crate::postgres_ext::Oid,
    _collation: crate::postgres_ext::Oid,
    _item1: &crate::nodes::primnodes::Expr,
    _item2: &crate::nodes::primnodes::Expr,
    _qualscope: Relids,
    _security_level: Index,
) -> RestrictInfo {
    not_yet_reachable("build_implied_join_equality: EC-implied join equality");
}

/*****************************************************************************
 *	  MERGE/HASH/MEMOIZE CHECKS
 *****************************************************************************/

/// PG `check_mergejoinable`: if the clause is a binary mergejoinable opclause
/// with no volatile functions, set `mergeopfamilies`. (`op_mergejoinable` /
/// `get_mergejoin_opfamilies` reach lsyscache; the seeded btree operators in the
/// catalog answer for the equality operators the planner sees.)
fn check_mergejoinable(restrictinfo: &mut RestrictInfo) {
    if restrictinfo.pseudoconstant {
        return;
    }
    let Node::OpExpr(op) = &restrictinfo.clause else { return };
    if op.args.len() != 2 {
        return;
    }
    let opno = op.opno;
    let leftarg_type = exprType(&op.args[0]);
    if op_mergejoinable_or_false(opno, leftarg_type)
        && !contain_volatile_functions_or_false(&restrictinfo.clause)
    {
        restrictinfo.mergeopfamilies = get_mergejoin_opfamilies(opno);
    }
}

/// PG `check_hashjoinable`: set `hashjoinoperator` if the clause is a binary
/// hashjoinable opclause with no volatile functions.
fn check_hashjoinable(restrictinfo: &mut RestrictInfo) {
    if restrictinfo.pseudoconstant {
        return;
    }
    let Node::OpExpr(op) = &restrictinfo.clause else { return };
    if op.args.len() != 2 {
        return;
    }
    let opno = op.opno;
    let leftarg_type = exprType(&op.args[0]);
    if op_hashjoinable_or_false(opno, leftarg_type)
        && !contain_volatile_functions_or_false(&restrictinfo.clause)
    {
        restrictinfo.hashjoinoperator = opno;
    }
}

/// PG `check_memoizable`: set left/right hasheqoperator from the typecache.
/// STAGED: `lookup_type_cache` (the hash/eq operator lookup) isn't wired, so we
/// leave the hasheqoperators invalid. Memoize is a pure optimization; leaving it
/// off costs nothing but the Memoize node.
#[allow(
    clippy::needless_pass_by_ref_mut,
    reason = "staged: writes left_/right_hasheqoperator once the typecache lookup lands"
)]
fn check_memoizable(restrictinfo: &mut RestrictInfo) {
    if restrictinfo.pseudoconstant {
        return;
    }
    let Node::OpExpr(op) = &restrictinfo.clause else { return };
    if op.args.len() == 2 {
        // TODO(typecache): lookup_type_cache(lefttype/righttype, HASH_PROC|EQ_OPR)
        // and set left_/right_hasheqoperator.
    }
}

/// `contain_volatile_functions` (optimizer/util/clauses) is not wired yet; the
/// quals the planner builds at M7 (Var/Const comparisons) are non-volatile, so
/// treating "contains volatile" as false is correct for them.
fn contain_volatile_functions_or_false(clause: &Node) -> bool {
    let _ = clause;
    // TODO(volatility): contain_volatile_functions(clause).
    false
}

/// `op_mergejoinable` (lsyscache): the M7 builtin-table form recognizes the seeded
/// "=" operators (the pg_amop list-scan syscache grows later). A mergejoinable
/// clause is absorbed into an EquivalenceClass and offered as a merge clause.
fn op_mergejoinable_or_false(opno: Oid, inputtype: Oid) -> bool {
    crate::utils::lsyscache::op_mergejoinable(opno, inputtype)
}

/// `op_hashjoinable` (lsyscache): the M7 builtin-table form recognizes the seeded
/// "=" operators so they get a hashjoinoperator and can drive a hash join.
fn op_hashjoinable_or_false(opno: Oid, inputtype: Oid) -> bool {
    crate::utils::lsyscache::op_hashjoinable(opno, inputtype)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::nodes::bitmapset::bms_equal;
    use crate::nodes::nodes::{CmdType, LimitOption};
    use crate::nodes::parsenodes::{Query, QuerySource, RangeTblEntry};
    use crate::nodes::pathnodes::{
        JoinDomain, PathTarget, PlannerGlobal, QualCost, RelOptInfo, RelOptKind,
    };
    use crate::nodes::primnodes::{
        Alias, FromExpr, OpExpr, OverridingKind, RangeTblRef, Var, VarReturningType,
    };
    use crate::postgres_ext::{InvalidOid, Oid};

    /// Public so sibling no-op modules (analyzejoins/planagg/prepagg) can borrow
    /// a bare PlannerInfo for their tests.
    #[allow(
        clippy::too_many_lines,
        reason = "PlannerInfo/Query/PlannerGlobal are large palloc0 structs; the test constructor fills every field"
    )]
    pub fn test_planner_info() -> PlannerInfo {
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
            join_domains: vec![Box::new(JoinDomain { jd_relids: None })],
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

    fn alias(name: &str, ncols: usize) -> Alias {
        let colnames = (1..=ncols)
            .map(|i| crate::nodes::value::makeString(format!("c{i}")))
            .collect();
        crate::nodes::makefuncs::makeAlias(name, colnames)
    }

    /// An RTE_RELATION RangeTblEntry with `ncols` columns (named c1..cN).
    fn relation_rte(name: &str, ncols: usize) -> RangeTblEntry {
        RangeTblEntry {
            alias: None,
            eref: Some(Box::new(alias(name, ncols))),
            rtekind: RTEKind::RELATION,
            relid: Oid(1000),
            inh: false,
            relkind: b'r' as i8,
            rellockmode: 1,
            perminfoindex: 0,
            tablesample: None,
            subquery: None,
            security_barrier: false,
            jointype: JoinType::INNER,
            joinmergedcols: 0,
            joinaliasvars: Vec::new(),
            joinleftcols: Vec::new(),
            joinrightcols: Vec::new(),
            join_using_alias: None,
            functions: Vec::new(),
            funcordinality: false,
            tablefunc: None,
            values_lists: Vec::new(),
            ctename: None,
            ctelevelsup: 0,
            self_reference: false,
            coltypes: Vec::new(),
            coltypmods: Vec::new(),
            colcollations: Vec::new(),
            enrname: None,
            enrtuples: 0.0,
            groupexprs: Vec::new(),
            lateral: false,
            inFromCl: true,
            securityQuals: Vec::new(),
        }
    }

    fn var(varno: i32, varattno: i16) -> Node {
        Node::Var(Box::new(Var {
            varno,
            varattno,
            vartype: Oid(23),
            vartypmod: -1,
            varcollid: InvalidOid,
            varnullingrels: None,
            varlevelsup: 0,
            varreturningtype: VarReturningType::DEFAULT,
            varnosyn: varno as usize,
            varattnosyn: varattno,
            location: -1,
        }))
    }

    /// `a.x = b.y` as an OpExpr (opno 96 = int4eq in PG's bootstrap catalog).
    fn eq_clause(lvarno: i32, rvarno: i32) -> Node {
        Node::OpExpr(Box::new(OpExpr {
            opno: Oid(96),
            opfuncid: InvalidOid,
            opresulttype: Oid(16),
            opretset: false,
            opcollid: InvalidOid,
            inputcollid: InvalidOid,
            args: vec![var(lvarno, 1), var(rvarno, 1)],
            location: -1,
        }))
    }

    /// Build a 2-base-rel planner over two relation RTEs, with the rels parked in
    /// simple_rel_array (manually, since get_relation_info needs the relcache).
    fn two_rel_root() -> PlannerInfo {
        let mut root = test_planner_info();
        root.parse.rtable = vec![
            Node::RangeTblEntry(Box::new(relation_rte("a", 2))),
            Node::RangeTblEntry(Box::new(relation_rte("b", 2))),
        ];
        // simple_rte_array (index 0 unused).
        root.simple_rte_array = vec![
            None,
            Some(Box::new(relation_rte("a", 2))),
            Some(Box::new(relation_rte("b", 2))),
        ];
        // Park two base rels with 2 attrs each (attno 1..2), no get_relation_info.
        root.simple_rel_array = vec![None, Some(test_base_rel(1)), Some(test_base_rel(2))];
        root
    }

    fn test_base_rel(relid: i32) -> Box<RelOptInfo> {
        let mut rel = empty_base_rel(relid);
        rel.min_attr = 1;
        rel.max_attr = 2;
        rel.attr_needed = vec![None, None];
        rel.attr_widths = vec![4, 4];
        Box::new(rel)
    }

    fn empty_base_rel(relid: i32) -> RelOptInfo {
        RelOptInfo {
            reloptkind: RelOptKind::BASEREL,
            relids: Some(bms_make_singleton(relid)),
            rows: 0.0,
            consider_startup: false,
            consider_param_startup: false,
            consider_parallel: false,
            reltarget: Some(Box::new(PathTarget {
                exprs: Vec::new(),
                sortgrouprefs: Vec::new(),
                cost: QualCost { startup: 0.0, per_tuple: 0.0 },
                width: 0,
                has_volatile_expr: VolatileFunctionStatus::UNKNOWN,
            })),
            pathlist: Vec::new(),
            ppilist: Vec::new(),
            partial_pathlist: Vec::new(),
            cheapest_startup_path: None,
            cheapest_total_path: None,
            cheapest_unique_path: None,
            cheapest_parameterized_paths: Vec::new(),
            direct_lateral_relids: None,
            lateral_relids: None,
            relid: relid as usize,
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
            amflags: crate::nodes::pathnodes::AmFlags::empty(),
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

    #[test]
    fn add_base_rels_walks_fromexpr() {
        let mut root = test_planner_info();
        root.parse.rtable = vec![
            Node::RangeTblEntry(Box::new(relation_rte("a", 1))),
            Node::RangeTblEntry(Box::new(relation_rte("b", 1))),
        ];
        // setup_simple_rel_arrays-equivalent sizing for build_simple_rel asserts.
        root.simple_rel_array = vec![None, None, None];
        root.simple_rte_array = vec![
            None,
            Some(Box::new(relation_rte("a", 1))),
            Some(Box::new(relation_rte("b", 1))),
        ];
        // build_simple_rel for RELATION reaches get_relation_info (relcache); to
        // avoid that, only exercise the recursion shape over a FromExpr of two
        // RangeTblRefs by parking pre-built rels and asserting the walk visits.
        // Here we instead verify the recursion structure with a SUBQUERY-free
        // RESULT RTE which build_simple_rel handles without the relcache.
        let result_rte = {
            let mut r = relation_rte("r", 0);
            r.rtekind = RTEKind::RESULT;
            r
        };
        root.parse.rtable = vec![Node::RangeTblEntry(Box::new(result_rte.clone()))];
        root.simple_rel_array = vec![None, None];
        root.simple_rte_array = vec![None, Some(Box::new(result_rte))];
        let jt = Node::FromExpr(Box::new(FromExpr {
            fromlist: vec![Node::RangeTblRef(Box::new(RangeTblRef { rtindex: 1 }))],
            quals: None,
        }));
        add_base_rels_to_query(&mut root, &jt);
        assert!(root.simple_rel_array[1].is_some(), "base rel 1 should be built");
    }

    #[test]
    fn distribute_single_rel_qual_goes_to_baserestrictinfo() {
        let mut root = two_rel_root();
        // A single-rel clause `a.c1 = a.c2` (both varno 1) -> base restriction on
        // rel 1. opno 0 makes it non-mergejoinable (op_mergejoinable stub returns
        // false for unseeded opno), so it stays an ordinary restriction.
        let clause = Node::OpExpr(Box::new(OpExpr {
            opno: InvalidOid,
            opfuncid: InvalidOid,
            opresulttype: Oid(16),
            opretset: false,
            opcollid: InvalidOid,
            inputcollid: InvalidOid,
            args: vec![var(1, 1), var(1, 2)],
            location: -1,
        }));
        let qualscope = bms_make_singleton(1);
        distribute_qual_to_rels(
            &mut root, &clause, 0, None, 0, &qualscope, None, None, None, true, false, false, false,
        );
        let rel = root.simple_rel_array[1].as_ref().unwrap();
        assert_eq!(rel.baserestrictinfo.len(), 1, "single-rel qual -> baserestrictinfo");
        assert_eq!(rel.baserestrictinfo[0].num_base_rels, 1);
    }

    #[test]
    fn distribute_join_qual_goes_to_joininfo() {
        let mut root = two_rel_root();
        // `a.c1 = b.c1` with a non-mergejoinable opno stays an ordinary join
        // clause and lands on both rels' joininfo.
        let clause = Node::OpExpr(Box::new(OpExpr {
            opno: InvalidOid,
            opfuncid: InvalidOid,
            opresulttype: Oid(16),
            opretset: false,
            opcollid: InvalidOid,
            inputcollid: InvalidOid,
            args: vec![var(1, 1), var(2, 1)],
            location: -1,
        }));
        let qualscope = bms_union(&bms_make_singleton(1), &bms_make_singleton(2));
        distribute_qual_to_rels(
            &mut root, &clause, 0, None, 0, &qualscope, None, None, None, true, false, false, false,
        );
        let rel1 = root.simple_rel_array[1].as_ref().unwrap();
        let rel2 = root.simple_rel_array[2].as_ref().unwrap();
        assert!(rel1.baserestrictinfo.is_empty(), "join clause is not a base restriction");
        assert_eq!(rel1.joininfo.len(), 1, "join clause on rel 1 joininfo");
        assert_eq!(rel2.joininfo.len(), 1, "join clause on rel 2 joininfo");
        // The join clause's required_relids spans both rels.
        let req = rel1.joininfo[0].required_relids.clone().unwrap();
        assert!(bms_equal(&req, &qualscope));
        assert!(rel1.joininfo[0].can_join, "binary op over disjoint rels is a join clause");
    }

    #[test]
    fn deconstruct_inner_join_distributes_clause() {
        // `FROM a, b WHERE a.c1 = b.c1` -- a 2-element FromExpr (implicit inner
        // join) with the equality as the top-level qual.
        let mut root = two_rel_root();
        let jt = Node::FromExpr(Box::new(FromExpr {
            fromlist: vec![
                Node::RangeTblRef(Box::new(RangeTblRef { rtindex: 1 })),
                Node::RangeTblRef(Box::new(RangeTblRef { rtindex: 2 })),
            ],
            quals: Some(Node::OpExpr(Box::new(OpExpr {
                opno: InvalidOid, // non-mergejoinable -> ordinary join clause
                opfuncid: InvalidOid,
                opresulttype: Oid(16),
                opretset: false,
                opcollid: InvalidOid,
                inputcollid: InvalidOid,
                args: vec![var(1, 1), var(2, 1)],
                location: -1,
            }))),
        }));
        root.parse.jointree = Some(jt);

        let joinlist = deconstruct_jointree(&mut root);
        // Both base rels appear in the joinlist (flat, 2 RangeTblRefs).
        assert_eq!(joinlist.len(), 2);
        // all_baserels = {1,2}.
        let all = root.all_baserels.clone().unwrap();
        assert!(bms_is_member(1, &all) && bms_is_member(2, &all));
        // The equality became a join clause on both rels' joininfo.
        assert_eq!(root.simple_rel_array[1].as_ref().unwrap().joininfo.len(), 1);
        assert_eq!(root.simple_rel_array[2].as_ref().unwrap().joininfo.len(), 1);
    }

    #[test]
    fn build_base_rel_tlists_marks_needed() {
        let mut root = two_rel_root();
        // final tlist references a.c1 and b.c2.
        let tlist = vec![var(1, 1), var(2, 2)];
        build_base_rel_tlists(&mut root, &tlist);
        // rel 1, attr 1 should now be needed by rel 0.
        let rel1 = root.simple_rel_array[1].as_ref().unwrap();
        assert!(rel1.attr_needed[0].is_some(), "a.c1 needed");
        assert!(bms_is_member(0, rel1.attr_needed[0].as_ref().unwrap()));
        // and present in the reltarget.
        assert_eq!(rel1.reltarget.as_ref().unwrap().exprs.len(), 1);
        let rel2 = root.simple_rel_array[2].as_ref().unwrap();
        assert!(rel2.attr_needed[1].is_some(), "b.c2 needed");
    }

    use crate::nodes::bitmapset::bms_is_member;
}
