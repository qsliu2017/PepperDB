//! Relation-node lookup/construction routines. Translated from
//! backend/optimizer/util/relnode.c.
//!
//! This is the keystone of join planning: it builds the per-relation
//! `RelOptInfo` structures and, given two of them, the `RelOptInfo` for their
//! join. For M7 the inner-join path is complete -- `build_simple_rel` builds a
//! base rel, `build_join_rel` builds the join rel (relids, reltarget, joininfo,
//! lateral_relids) for `a JOIN b ON a.x=b.y`.
//!
//! Representation note (the `find_base_rel` owned-clone convention):
//! `simple_rel_array` holds `Option<Box<RelOptInfo>>`. The committed header
//! signature has `find_base_rel` (and friends) return an *owned* `Box` -- a
//! clone of the parked rel, NOT a borrow. Consequently mutations to the
//! returned rel do not flow back into `root.simple_rel_array`; a caller that
//! must mutate the parked rel (e.g. appending to its `joininfo`) writes back to
//! `root.simple_rel_array[relid]` explicitly. `build_join_rel` reads its
//! `outer_rel`/`inner_rel` by `&RelOptInfo` (the caller already holds them) and
//! builds a fresh `RelOptInfo` that it pushes into `root.join_rel_list`.
//!
//! `join_rel_hash` (PG's HTAB) was dropped from the skeleton; `find_join_rel`
//! does a linear scan of `join_rel_list` comparing relids with `bms_equal`.
//!
//! Staged for later milestones (route through `not_yet_reachable`, rules.md s4):
//! the parameterized-path bodies (`get_baserel_parampathinfo` etc) past their
//! `required_outer.is_empty()` fast paths, partitionwise join
//! (`build_child_join_rel`, `find_childrel_parents`), and the size/row estimate
//! (`set_joinrel_size_estimates` -> selfuncs, step 31). `build_join_rel` sets a
//! pre-selectivity Cartesian `rows = outer.rows * inner.rows` placeholder.

#![allow(
    clippy::needless_pass_by_value,
    reason = "1:1 PG port: signatures take owned node/relids values matching PG C; some are consumed only once deferred paths land"
)]

use crate::nodes::bitmapset::{
    bms_copy, bms_del_members, bms_equal, bms_is_subset, bms_make_singleton, bms_nonempty_difference,
    bms_num_members, bms_union,
};
use crate::nodes::nodes::{Cardinality, JoinType, Node};
use crate::nodes::parsenodes::RTEKind;
use crate::nodes::pathnodes::{
    AmFlags, ParamPathInfo, PathTarget, PlannerInfo, QualCost, RelOptInfo, RelOptKind, Relids,
    SpecialJoinInfo, UpperRelationKind, VolatileFunctionStatus,
};
use crate::nodes::primnodes::{Index, Var};
use crate::optimizer::optimizer::clamp_width_est;
use crate::postgres_ext::InvalidOid;

/// Panic for a relnode path not yet translated for this milestone (rules.md s4).
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

/// Is a relids set empty? (PG `bms_is_empty`; not yet a standalone bms helper.)
fn relids_is_empty(a: &Relids) -> bool {
    bms_num_members(a) == 0
}

/// PG `create_empty_pathtarget`: a zeroed PathTarget. (The re-export in
/// `crate::optimizer::tlist` is still an `unimplemented!()` stub, so we build it
/// directly here.)
fn create_empty_pathtarget() -> PathTarget {
    PathTarget {
        exprs: Vec::new(),
        sortgrouprefs: Vec::new(),
        cost: QualCost { startup: 0.0, per_tuple: 0.0 },
        width: 0,
        has_volatile_expr: VolatileFunctionStatus::UNKNOWN,
    }
}

/// PG `makeNode(RelOptInfo)` with palloc0 semantics: every field zero/empty.
/// `build_simple_rel`/`build_join_rel`/`fetch_upper_rel` then overwrite the
/// fields that matter for their case. Mirrors planmain's `make_result_rel`
/// field-for-field; kept local so this module is self-contained.
pub(crate) fn make_node_reloptinfo(reloptkind: RelOptKind) -> RelOptInfo {
    RelOptInfo {
        reloptkind,
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

/// PG `setup_simple_rel_arrays`: size `simple_rel_array`/`simple_rte_array` to
/// `rtable.len() + 1` (RT indexes are 1..N; entry 0 wasted), filling the RTE
/// array from the rtable. `append_rel_array` is left empty when there are no
/// AppendRelInfos (the only case for M7).
pub fn setup_simple_rel_arrays(root: &mut PlannerInfo) {
    let size = root.parse.rtable.len() + 1;
    root.simple_rel_array = (0..size).map(|_| None).collect();
    root.simple_rte_array = Vec::with_capacity(size);
    root.simple_rte_array.push(None);
    for rte in &root.parse.rtable {
        let Node::RangeTblEntry(rte) = rte else {
            not_yet_reachable("setup_simple_rel_arrays: rangetable entry is not an RTE");
        };
        root.simple_rte_array.push(Some(rte.clone()));
    }

    if root.append_rel_list.is_empty() {
        root.append_rel_array = Vec::new();
        return;
    }
    not_yet_reachable("setup_simple_rel_arrays: append_rel_list (UNION ALL flattening)");
}

/// PG `expand_planner_arrays`: grow the per-RTE arrays by `add_size`, padding
/// the new slots with NULLs. Only reached during inheritance/UNION-ALL child
/// expansion, which M7 does not exercise.
pub fn expand_planner_arrays(_root: &mut PlannerInfo, add_size: i32) {
    crate::assert!(add_size > 0);
    not_yet_reachable("expand_planner_arrays: child relation expansion");
}

/// PG `build_simple_rel`: construct a base `RelOptInfo` for `relid` and park it
/// in `root.simple_rel_array[relid]`. For an `RTE_RELATION` we fill the
/// attribute/size fields from the relcache via `get_relation_info`; the other
/// RTE kinds set up the attr arrays directly. `parent` (appendrel inheritance)
/// is staged. Returns an owned clone of the parked rel (matching the committed
/// header convention; see the module doc).
pub fn build_simple_rel(
    root: &mut PlannerInfo,
    relid: i32,
    parent: Option<&RelOptInfo>,
) -> Box<RelOptInfo> {
    crate::assert!(relid > 0 && (relid as usize) < root.simple_rel_array.len());
    crate::assert!(root.simple_rel_array[relid as usize].is_none());

    if parent.is_some() {
        not_yet_reachable("build_simple_rel: appendrel child (parent != NULL)");
    }

    let rte = root.simple_rte_array[relid as usize]
        .clone()
        .unwrap_or_else(|| not_yet_reachable("build_simple_rel: missing RTE"));

    let mut rel = make_node_reloptinfo(RelOptKind::BASEREL);
    rel.relids = Some(bms_make_singleton(relid));
    rel.rows = 0.0;
    rel.consider_startup = root.tuple_fraction > 0.0;
    rel.reltarget = Some(Box::new(create_empty_pathtarget()));
    rel.relid = relid as usize;
    rel.rtekind = rte.rtekind;
    rel.rel_parallel_workers = -1;
    rel.serverid = InvalidOid;
    rel.userid = InvalidOid;

    match rte.rtekind {
        RTEKind::RELATION => {
            // attr range / arrays are filled by get_relation_info below.
            crate::backend::optimizer::util::plancat::get_relation_info(
                root, rte.relid, rte.inh, &mut rel,
            );
        }
        RTEKind::SUBQUERY
        | RTEKind::FUNCTION
        | RTEKind::TABLEFUNC
        | RTEKind::VALUES
        | RTEKind::CTE
        | RTEKind::NAMEDTUPLESTORE => {
            // 0 is included in the range to support whole-row Vars.
            rel.min_attr = 0;
            let ncols = rte
                .eref
                .as_ref()
                .map_or(0, |a| a.colnames.len());
            rel.max_attr = ncols as i16;
            let span = (rel.max_attr - rel.min_attr + 1) as usize;
            rel.attr_needed = vec![None; span];
            rel.attr_widths = vec![0; span];
        }
        RTEKind::RESULT => {
            rel.min_attr = 0;
            rel.max_attr = -1;
            rel.attr_needed = Vec::new();
            rel.attr_widths = Vec::new();
        }
        _ => not_yet_reachable("build_simple_rel: unrecognized RTE kind"),
    }

    // Park before any child-qual transformation could need to find it.
    root.simple_rel_array[relid as usize] = Some(Box::new(rel.clone()));
    Box::new(rel)
}

/// PG `find_base_rel`: the base/otherrel entry for `relid`, which must exist.
/// Returns an owned clone (see module doc on the owned-clone convention).
pub fn find_base_rel(root: &mut PlannerInfo, relid: i32) -> Box<RelOptInfo> {
    if (relid as u32) < root.simple_rel_array.len() as u32
        && let Some(rel) = &root.simple_rel_array[relid as usize]
    {
        return rel.clone();
    }
    not_yet_reachable("find_base_rel: no relation entry for relid");
}

/// PG `find_base_rel_noerr`: like `find_base_rel` but returns None if absent.
pub fn find_base_rel_noerr(root: &mut PlannerInfo, relid: i32) -> Option<Box<RelOptInfo>> {
    if (relid as u32) < root.simple_rel_array.len() as u32 {
        return root.simple_rel_array[relid as usize].clone();
    }
    None
}

/// PG `find_base_rel_ignore_join`: like `find_base_rel`, but returns None
/// (rather than erroring) when `relid` references an outer join. Convenient for
/// callers iterating relid sets that mix base rels and OJ relids.
pub fn find_base_rel_ignore_join(root: &mut PlannerInfo, relid: i32) -> Box<RelOptInfo> {
    if (relid as u32) < root.simple_rel_array.len() as u32 {
        if let Some(rel) = &root.simple_rel_array[relid as usize] {
            return rel.clone();
        }
        if let Some(rte) = &root.simple_rte_array[relid as usize]
            && rte.rtekind == RTEKind::JOIN
            && rte.jointype != JoinType::INNER
        {
            not_yet_reachable("find_base_rel_ignore_join: outer-join relid (returns None)");
        }
    }
    not_yet_reachable("find_base_rel_ignore_join: no relation entry for relid");
}

/// PG `find_join_rel`: the join `RelOptInfo` whose relids equal `relids`, or
/// None. Linear scan of `join_rel_list` (the HTAB was dropped from the
/// skeleton; fine through M7).
pub fn find_join_rel(root: &mut PlannerInfo, relids: Relids) -> Option<Box<RelOptInfo>> {
    root.join_rel_list
        .iter()
        .find(|rel| {
            rel.relids
                .as_ref()
                .is_some_and(|r| bms_equal(r, &relids))
        })
        .cloned()
}

/// PG `min_join_parameterization`: the minimum parameterization of a joinrel --
/// the union of the inputs' `lateral_relids`, minus whatever is already in the
/// join. (Transitive closure and PHV lateral refs are already folded into the
/// inputs' lateral_relids, so no extra work here.)
pub fn min_join_parameterization(
    _root: &mut PlannerInfo,
    joinrelids: Relids,
    outer_rel: &RelOptInfo,
    inner_rel: &RelOptInfo,
) -> Relids {
    let outer = outer_rel.lateral_relids.clone().unwrap_or_default();
    let inner = inner_rel.lateral_relids.clone().unwrap_or_default();
    let result = bms_union(&outer, &inner);
    bms_del_members(result, &joinrelids)
}

/// PG `build_joinrel_tlist` (one input rel at a time, invoked twice): add to the
/// joinrel's reltarget every Var of `input_rel` that is still needed above this
/// join (per the base rel's `attr_needed`), accumulating the output width.
///
/// `can_null` (the input is on the nullable side of an outer join) and the
/// `pushed_down_joins` varnullingrels bookkeeping only matter for outer joins;
/// for the inner-join keystone `can_null` is false and that logic is skipped.
/// PlaceHolderVars in the input target route to `not_yet_reachable` (none exist
/// for inner-join queries; `add_placeholders_to_joinrel` handles real PHVs).
fn build_joinrel_tlist(
    root: &mut PlannerInfo,
    joinrel: &mut RelOptInfo,
    input_rel: &RelOptInfo,
    _sjinfo: &SpecialJoinInfo,
    _pushed_down_joins: &[Node],
    can_null: bool,
) {
    let relids = joinrel.relids.clone().unwrap_or_default();
    let mut tuple_width = i64::from(
        joinrel
            .reltarget
            .as_ref()
            .unwrap_or_else(|| not_yet_reachable("build_joinrel_tlist: missing joinrel reltarget"))
            .width,
    );

    let input_exprs = input_rel
        .reltarget
        .as_ref()
        .unwrap_or_else(|| not_yet_reachable("build_joinrel_tlist: missing input reltarget"))
        .exprs
        .clone();

    for expr in input_exprs {
        let var = match &expr {
            Node::Var(v) => v.as_ref().clone(),
            Node::PlaceHolderVar(_) => {
                not_yet_reachable("build_joinrel_tlist: PlaceHolderVar in input reltarget")
            }
            _ => not_yet_reachable("build_joinrel_tlist: unexpected node type in rel targetlist"),
        };

        if var.varno == crate::nodes::primnodes::ROWID_VAR {
            // UPDATE/DELETE/MERGE row identity vars are always needed.
            not_yet_reachable("build_joinrel_tlist: ROWID_VAR row identity var");
        }

        // Get the Var's original base rel and check whether it's still needed
        // above this joinrel.
        let baserel = find_base_rel(root, var.varno);
        let ndx = (var.varattno - baserel.min_attr) as usize;
        let attr_needed = baserel.attr_needed.get(ndx).and_then(Clone::clone).unwrap_or_default();
        if !bms_nonempty_difference(&attr_needed, &relids) {
            continue; // not needed above this join, skip it
        }
        tuple_width += i64::from(*baserel.attr_widths.get(ndx).unwrap_or(&0));

        let out_var = if can_null {
            // Outer-join varnullingrels bookkeeping is deferred (inner join:
            // can_null is false, so we never get here).
            not_yet_reachable("build_joinrel_tlist: can_null Var nullingrels");
        } else {
            push_var(&var)
        };

        if let Some(target) = joinrel.reltarget.as_mut() {
            target.exprs.push(out_var);
        }
    }

    if let Some(target) = joinrel.reltarget.as_mut() {
        target.width = clamp_width_est(tuple_width);
    }
}

/// Clone a Var into a `Node` for the joinrel target list.
fn push_var(var: &Var) -> Node {
    Node::Var(Box::new(var.clone()))
}

/// PG `build_joinrel_restrictlist`: the restriction clauses that apply to this
/// particular pair of input rels -- the join clauses from the inputs' joininfo
/// that now refer to no rels outside the joinrel, plus EC-derived join
/// equalities. The structural collection from the inputs' joininfo is
/// translated; the EC-derived part (`generate_join_implied_equalities`) is
/// staged (equivclass.rs, a concurrent file). For the M7 single-clause inner
/// join `a JOIN b ON a.x=b.y` the ON clause is distributed into both inputs'
/// joininfo and collected here.
fn build_joinrel_restrictlist(
    _root: &mut PlannerInfo,
    joinrel: &RelOptInfo,
    outer_rel: &RelOptInfo,
    inner_rel: &RelOptInfo,
    _sjinfo: &SpecialJoinInfo,
) -> Vec<Node> {
    let joinrelids = joinrel.relids.clone().unwrap_or_default();
    let both = bms_union(
        &outer_rel.relids.clone().unwrap_or_default(),
        &inner_rel.relids.clone().unwrap_or_default(),
    );

    let mut result: Vec<Node> = Vec::new();
    subbuild_joinrel_restrictlist(&joinrelids, outer_rel, &both, &mut result);
    subbuild_joinrel_restrictlist(&joinrelids, inner_rel, &both, &mut result);

    // EC-derived join equalities (generate_join_implied_equalities) are added
    // here in PG; that machinery lands with equivclass.rs.
    result
}

/// PG `subbuild_joinrel_restrictlist`: append, de-duplicating by serial, the
/// clauses from `input_rel.joininfo` whose required_relids are now a subset of
/// the joinrel's relids (so they become restriction clauses here). Clauses that
/// still reference outside rels stay join clauses and are ignored. The clone /
/// clone-incompatibility checks only fire for outer-join clones (staged).
fn subbuild_joinrel_restrictlist(
    joinrelids: &Relids,
    input_rel: &RelOptInfo,
    _both_input_relids: &Relids,
    result: &mut Vec<Node>,
) {
    for rinfo in &input_rel.joininfo {
        let required = rinfo.required_relids.clone().unwrap_or_default();
        if bms_is_subset(&required, joinrelids) {
            if rinfo.has_clone || rinfo.is_clone {
                not_yet_reachable("subbuild_joinrel_restrictlist: clone (outer-join) clause");
            }
            let node = Node::RestrictInfo(Box::new(rinfo.as_ref().clone()));
            if !result.iter().any(|existing| restrictinfo_serial_eq(existing, rinfo.rinfo_serial)) {
                result.push(node);
            }
        }
    }
}

/// Pointer-equality stand-in for `list_append_unique_ptr`: PG dedups by pointer
/// identity, which we approximate by the unique `rinfo_serial`.
fn restrictinfo_serial_eq(node: &Node, serial: i32) -> bool {
    matches!(node, Node::RestrictInfo(ri) if ri.rinfo_serial == serial)
}

/// PG `build_joinrel_joinlist`: the join clauses that remain join clauses above
/// this joinrel -- from both inputs' joininfo, those still referencing outside
/// rels. Stored into `joinrel.joininfo`.
fn build_joinrel_joinlist(
    joinrel: &mut RelOptInfo,
    outer_rel: &RelOptInfo,
    inner_rel: &RelOptInfo,
) {
    crate::assert!(joinrel.reloptkind == RelOptKind::JOINREL);
    let joinrelids = joinrel.relids.clone().unwrap_or_default();
    let mut result: Vec<crate::nodes::pathnodes::RestrictInfo> = Vec::new();
    subbuild_joinrel_joinlist(&joinrelids, &outer_rel.joininfo, &mut result);
    subbuild_joinrel_joinlist(&joinrelids, &inner_rel.joininfo, &mut result);
    joinrel.joininfo = result.into_iter().map(Box::new).collect();
}

fn subbuild_joinrel_joinlist(
    joinrelids: &Relids,
    joininfo_list: &[Box<crate::nodes::pathnodes::RestrictInfo>],
    new_joininfo: &mut Vec<crate::nodes::pathnodes::RestrictInfo>,
) {
    for rinfo in joininfo_list {
        let required = rinfo.required_relids.clone().unwrap_or_default();
        if bms_is_subset(&required, joinrelids) {
            // Becomes a restriction clause at this level; ignore here.
        } else if !new_joininfo.iter().any(|e| e.rinfo_serial == rinfo.rinfo_serial) {
            new_joininfo.push(rinfo.as_ref().clone());
        }
    }
}

/// PG `build_join_rel` (the keystone). Returns the join `RelOptInfo` for the
/// union of `outer_rel` and `inner_rel`, building it if it does not already
/// exist, plus the restriction clauses for this input pair (the C out-param
/// `restrictlist_ptr`).
///
/// Structural build is complete: relids = outer | inner (`joinrelids`),
/// reloptkind = JOINREL, lateral_relids via `min_join_parameterization`,
/// reltarget via `build_joinrel_tlist` over both inputs, joininfo via
/// `build_joinrel_joinlist`. The row estimate is staged: rather than call the
/// selfuncs selectivity machinery (`set_joinrel_size_estimates`, step 31) we
/// set the pre-selectivity Cartesian `rows = outer.rows * inner.rows`.
pub fn build_join_rel(
    root: &mut PlannerInfo,
    joinrelids: Relids,
    outer_rel: &RelOptInfo,
    inner_rel: &RelOptInfo,
    sjinfo: &SpecialJoinInfo,
    pushed_down_joins: Vec<Node>,
) -> (Box<RelOptInfo>, Vec<Node>) {
    crate::assert!(!matches!(
        outer_rel.reloptkind,
        RelOptKind::OTHER_MEMBER_REL | RelOptKind::OTHER_JOINREL
    ));
    crate::assert!(!matches!(
        inner_rel.reloptkind,
        RelOptKind::OTHER_MEMBER_REL | RelOptKind::OTHER_JOINREL
    ));

    // Already have a joinrel for this set of base rels?
    if let Some(existing) = find_join_rel(root, joinrelids.clone()) {
        let restrictlist =
            build_joinrel_restrictlist(root, &existing, outer_rel, inner_rel, sjinfo);
        return (existing, restrictlist);
    }

    // Make a fresh one.
    let mut joinrel = make_node_reloptinfo(RelOptKind::JOINREL);
    joinrel.relids = Some(bms_copy(&joinrelids));
    joinrel.rows = 0.0;
    joinrel.consider_startup = root.tuple_fraction > 0.0;
    joinrel.reltarget = Some(Box::new(create_empty_pathtarget()));
    joinrel.direct_lateral_relids = Some(bms_union(
        &outer_rel.direct_lateral_relids.clone().unwrap_or_default(),
        &inner_rel.direct_lateral_relids.clone().unwrap_or_default(),
    ));
    joinrel.lateral_relids =
        Some(min_join_parameterization(root, joinrelids.clone(), outer_rel, inner_rel));
    joinrel.relid = 0; // not a baserel
    joinrel.rtekind = RTEKind::JOIN;
    joinrel.serverid = InvalidOid;
    joinrel.userid = InvalidOid;

    // set_foreign_rel_properties: only matters for foreign tables; for ordinary
    // tables both serverids are invalid, so this is a no-op (skipped).

    // Fill the joinrel's tlist with the Vars/PHVs needed above this join.
    build_joinrel_tlist(
        root,
        &mut joinrel,
        outer_rel,
        sjinfo,
        &pushed_down_joins,
        sjinfo.jointype == JoinType::FULL,
    );
    build_joinrel_tlist(
        root,
        &mut joinrel,
        inner_rel,
        sjinfo,
        &pushed_down_joins,
        sjinfo.jointype != JoinType::INNER,
    );
    crate::backend::optimizer::util::placeholder::add_placeholders_to_joinrel(
        root, &mut joinrel, outer_rel, inner_rel, sjinfo,
    );

    // Finish direct_lateral_relids: strip the join's own relids.
    let dlr = joinrel.direct_lateral_relids.take().unwrap_or_default();
    joinrel.direct_lateral_relids = Some(bms_del_members(dlr, &joinrelids));

    // Restrict and join clause lists.
    let restrictlist =
        build_joinrel_restrictlist(root, &joinrel, outer_rel, inner_rel, sjinfo);
    build_joinrel_joinlist(&mut joinrel, outer_rel, inner_rel);

    // has_relevant_eclass_joinclause (equivclass.rs); no eclass joins for the
    // bare M7 inner join, so leave has_eclass_joins false.
    joinrel.has_eclass_joins = false;

    // build_joinrel_partition_info: partitionwise join is staged (M7 has no
    // partitioned rels); leave partition fields unset.

    // set_joinrel_size_estimates (selfuncs, step 31) is staged. Use the
    // pre-selectivity Cartesian product as a conservative row estimate.
    // TODO(selfuncs step 31): replace with set_joinrel_size_estimates.
    joinrel.rows = (outer_rel.rows as Cardinality) * (inner_rel.rows as Cardinality);

    // consider_parallel: requires is_parallel_safe over the restrictlist and
    // target (clauses.rs); left false until that lands.

    // Add the joinrel to the PlannerInfo (GEQO requires append to the end).
    root.join_rel_list.push(Box::new(joinrel.clone()));

    // join_rel_level DP bookkeeping is unused on the M7 path (join_rel_level
    // empty); skipped.

    (Box::new(joinrel), restrictlist)
}

/// PG `fetch_upper_rel`: the `RelOptInfo` for a post-scan/join processing stage
/// (`kind`, `relids`), creating it if absent. Only the fields add_path/
/// set_cheapest read are set.
pub fn fetch_upper_rel(
    root: &mut PlannerInfo,
    kind: UpperRelationKind,
    relids: Relids,
) -> Box<RelOptInfo> {
    let slot = kind as usize;
    if let Some(existing) = root.upper_rels[slot].iter().find(|r| {
        r.relids.as_ref().is_some_and(|r| bms_equal(r, &relids))
    }) {
        return existing.clone();
    }

    let mut upperrel = make_node_reloptinfo(RelOptKind::UPPER_REL);
    upperrel.relids = Some(bms_copy(&relids));
    upperrel.consider_startup = root.tuple_fraction > 0.0;
    upperrel.reltarget = Some(Box::new(create_empty_pathtarget()));

    root.upper_rels[slot].push(Box::new(upperrel.clone()));
    Box::new(upperrel)
}

/// PG `find_childrel_parents`: parent relids of an appendrel child. Staged
/// (appendrel/partitioning not in M7).
pub fn find_childrel_parents(_root: &mut PlannerInfo, _rel: &RelOptInfo) -> Relids {
    not_yet_reachable("find_childrel_parents: appendrel child");
}

/// PG `get_baserel_parampathinfo`: the cached `ParamPathInfo` for a
/// parameterized base-rel path. The unparameterized fast path (empty
/// `required_outer` => None) is translated; the parameterized body
/// (movable-joinclause identification + size estimate) is staged.
pub fn get_baserel_parampathinfo(
    _root: &mut PlannerInfo,
    baserel: &RelOptInfo,
    required_outer: Relids,
) -> Option<Box<ParamPathInfo>> {
    crate::assert!(bms_is_subset(
        &baserel.lateral_relids.clone().unwrap_or_default(),
        &required_outer
    ));
    if relids_is_empty(&required_outer) {
        return None;
    }
    not_yet_reachable("get_baserel_parampathinfo: parameterized base-rel path");
}

/// PG `get_joinrel_parampathinfo`: the cached `ParamPathInfo` for a
/// parameterized join path. Unparameterized fast path (empty `required_outer`
/// => None, no extra clauses) translated; the parameterized body staged. The C
/// out-param `restrict_clauses` becomes the second tuple element (unchanged on
/// the fast path).
pub fn get_joinrel_parampathinfo(
    _root: &mut PlannerInfo,
    joinrel: &RelOptInfo,
    _outer_path: &crate::nodes::pathnodes::Path,
    _inner_path: &crate::nodes::pathnodes::Path,
    _sjinfo: &SpecialJoinInfo,
    required_outer: Relids,
) -> (Option<Box<ParamPathInfo>>, Vec<Node>) {
    crate::assert!(bms_is_subset(
        &joinrel.lateral_relids.clone().unwrap_or_default(),
        &required_outer
    ));
    if relids_is_empty(&required_outer) {
        return (None, Vec::new());
    }
    not_yet_reachable("get_joinrel_parampathinfo: parameterized join path");
}

/// PG `get_appendrel_parampathinfo`: a `ParamPathInfo` flagging an Append path
/// as parameterized. Unparameterized fast path translated; the parameterized
/// body staged (appendrels not in M7).
pub fn get_appendrel_parampathinfo(
    appendrel: &RelOptInfo,
    required_outer: Relids,
) -> Option<Box<ParamPathInfo>> {
    crate::assert!(bms_is_subset(
        &appendrel.lateral_relids.clone().unwrap_or_default(),
        &required_outer
    ));
    if relids_is_empty(&required_outer) {
        return None;
    }
    not_yet_reachable("get_appendrel_parampathinfo: parameterized append path");
}

/// PG `find_param_path_info`: the rel's existing `ParamPathInfo` matching
/// `required_outer`, or None.
pub fn find_param_path_info(
    rel: &RelOptInfo,
    required_outer: Relids,
) -> Option<Box<ParamPathInfo>> {
    rel.ppilist
        .iter()
        .find(|ppi| {
            ppi.req_outer
                .as_ref()
                .map_or_else(|| relids_is_empty(&required_outer), |ro| bms_equal(ro, &required_outer))
        })
        .cloned()
}

/// PG `get_param_path_clause_serials`: the set of pushed-down clause serials
/// enforced within a parameterized path. Staged (parameterized paths not in
/// M7).
pub fn get_param_path_clause_serials(path: &crate::nodes::pathnodes::Path) -> crate::nodes::bitmapset::Bitmapset {
    if path.param_info.is_none() {
        return crate::nodes::bitmapset::Bitmapset::default();
    }
    not_yet_reachable("get_param_path_clause_serials: parameterized path");
}

/// PG `build_child_join_rel`: the join `RelOptInfo` between two child rels (one
/// partition each). Staged (partitionwise join not in M7).
pub fn build_child_join_rel(
    _root: &mut PlannerInfo,
    _outer_rel: &RelOptInfo,
    _inner_rel: &RelOptInfo,
    _parent_joinrel: &RelOptInfo,
    _restrictlist: Vec<Node>,
    _sjinfo: &SpecialJoinInfo,
    _appinfos: &[&crate::nodes::pathnodes::AppendRelInfo],
) -> Box<RelOptInfo> {
    not_yet_reachable("build_child_join_rel: partitionwise child join");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::pathnodes::PathTarget;

    fn empty_pathtarget_with(exprs: Vec<Node>) -> PathTarget {
        PathTarget {
            exprs,
            sortgrouprefs: Vec::new(),
            cost: QualCost { startup: 0.0, per_tuple: 0.0 },
            width: 0,
            has_volatile_expr: VolatileFunctionStatus::UNKNOWN,
        }
    }

    /// A Var referencing base rel `varno`, attribute `varattno`.
    fn make_var(varno: i32, varattno: i16) -> Node {
        Node::Var(Box::new(Var {
            varno,
            varattno,
            vartype: crate::postgres_ext::Oid(23),
            vartypmod: -1,
            varcollid: InvalidOid,
            varnullingrels: None,
            varlevelsup: 0,
            varreturningtype: crate::nodes::primnodes::VarReturningType::DEFAULT,
            varnosyn: varno as Index,
            varattnosyn: varattno,
            location: -1,
        }))
    }

    /// A base RelOptInfo for `relid` whose reltarget holds one Var (attno 1) and
    /// whose attr_needed marks that column as needed above the join {1,2} (the
    /// extra relid 3 stands in for the final query output, so
    /// `bms_nonempty_difference(attr_needed, joinrelids)` is true and the Var is
    /// carried up into the joinrel target).
    fn make_test_base_rel(relid: i32, _join_relids: &Relids) -> RelOptInfo {
        let mut rel = make_node_reloptinfo(RelOptKind::BASEREL);
        rel.relids = Some(bms_make_singleton(relid));
        rel.relid = relid as usize;
        rel.rows = 10.0;
        rel.min_attr = 1;
        rel.max_attr = 1;
        let needed_above = bms_union(&bms_make_singleton(relid), &bms_make_singleton(3));
        rel.attr_needed = vec![Some(needed_above)];
        rel.attr_widths = vec![4];
        rel.reltarget = Some(Box::new(empty_pathtarget_with(vec![make_var(relid, 1)])));
        rel
    }

    fn inner_sjinfo(joinrelids: &Relids) -> SpecialJoinInfo {
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

    /// Minimal PlannerInfo with two base rels parked at simple_rel_array[1] and
    /// [2]. build_join_rel reads only tuple_fraction, simple_rel_array,
    /// placeholder_list, glob.last_phid, join_rel_list and upper_rels, all set
    /// to their zero/empty state here.
    fn planner_with_two_base_rels() -> (PlannerInfo, Relids) {
        let joinrelids = bms_union(&bms_make_singleton(1), &bms_make_singleton(2));
        let rel1 = make_test_base_rel(1, &joinrelids);
        let rel2 = make_test_base_rel(2, &joinrelids);

        let mut root = test_planner_info();
        root.simple_rel_array = vec![None, Some(Box::new(rel1)), Some(Box::new(rel2))];
        root.simple_rte_array = vec![None, None, None];
        (root, joinrelids)
    }

    /// A bare PlannerInfo sufficient for build_join_rel: tuple_fraction 0, empty
    /// placeholder_list (so add_placeholders_to_joinrel is a no-op), empty
    /// join_rel_list / upper_rels.
    #[allow(
        clippy::too_many_lines,
        reason = "PlannerInfo/Query/PlannerGlobal are large palloc0 structs; the test constructor must fill every field"
    )]
    fn test_planner_info() -> PlannerInfo {
        use crate::nodes::nodes::{CmdType, LimitOption};
        use crate::nodes::parsenodes::{Query, QuerySource};
        use crate::nodes::pathnodes::PlannerGlobal;
        use crate::nodes::primnodes::OverridingKind;

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
            join_domains: Vec::new(),
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
    fn build_join_rel_of_two_base_rels() {
        let (mut root, joinrelids) = planner_with_two_base_rels();
        let outer = root.simple_rel_array[1].clone().unwrap();
        let inner = root.simple_rel_array[2].clone().unwrap();
        let sjinfo = inner_sjinfo(&joinrelids);

        let (joinrel, _restrict) =
            build_join_rel(&mut root, joinrelids.clone(), &outer, &inner, &sjinfo, Vec::new());

        // relids == {1,2}
        assert!(bms_equal(
            joinrel.relids.as_ref().unwrap(),
            &joinrelids
        ));
        assert_eq!(joinrel.reloptkind, RelOptKind::JOINREL);
        // both inputs' Vars are still needed above the join -> reltarget has 2 exprs.
        let reltarget = joinrel.reltarget.as_ref().unwrap();
        assert_eq!(reltarget.exprs.len(), 2);
        // pre-selectivity Cartesian row estimate.
        assert!((joinrel.rows - 100.0).abs() < 1e-9);
        // pushed into join_rel_list.
        assert_eq!(root.join_rel_list.len(), 1);
        // find_join_rel locates it.
        assert!(find_join_rel(&mut root, joinrelids).is_some());
    }

    #[test]
    fn find_base_rel_returns_parked_clone() {
        let (mut root, joinrelids) = planner_with_two_base_rels();
        let rel = find_base_rel(&mut root, 1);
        assert!(bms_equal(rel.relids.as_ref().unwrap(), &bms_make_singleton(1)));
        let _ = joinrelids;
        assert!(find_base_rel_noerr(&mut root, 9).is_none());
    }
}
