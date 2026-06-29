//! Routines to generate index paths and create_index_paths(). Translated from the
//! M6-reachable parts of `backend/optimizer/path/indxpath.c` (disposition: grow).
//!
//! `create_index_paths` is the planner's index-path entry: for each index on the
//! base relation, match the WHERE restriction clauses to the index columns
//! (`match_clause_to_index` -> `match_clause_to_indexcol`), build the index quals,
//! and -- if any clause matched -- construct an IndexPath (and a BitmapHeapPath over
//! it) costed by `cost_index` / `cost_bitmap_heap_scan` so the planner can pick the
//! index over a seqscan when it is selective.
//!
//! M6 scope: a single base relation, a single-/multi-column btree index, and
//! `indexcol op const` equality/range restriction clauses. Join clauses (M7), OR
//! clauses, ScalarArrayOp, RowCompare, index ORDER BY, partial-index predicate
//! proof, and the AND/OR bitmap trees are grow guards (rules.md s4).

#![allow(
    clippy::vec_box,
    reason = "1:1 PG port: List* of IndexClause/RestrictInfo pointers maps to Vec<Box<_>> (matches pathnodes types)"
)]

use crate::access::stratnum::{
    StrategyNumber, BT_EQUAL_STRATEGY_NUMBER, BT_GREATER_EQUAL_STRATEGY_NUMBER,
    BT_GREATER_STRATEGY_NUMBER, BT_LESS_EQUAL_STRATEGY_NUMBER, BT_LESS_STRATEGY_NUMBER,
};
use crate::access::sdir::ScanDirection;
use crate::nodes::nodes::Node;
use crate::nodes::pathnodes::{
    IndexClause, IndexOptInfo, PlannerInfo, RelOptInfo, RestrictInfo,
};
use crate::nodes::primnodes::INDEX_VAR;

use crate::backend::optimizer::util::pathnode::{
    add_path, create_bitmap_heap_path, create_index_path,
};

/// PG `create_index_paths`: generate all useful index paths for `rel`. For each
/// index, match the rel's restriction clauses to the index columns; when at least
/// one clause matches, add an IndexPath and a BitmapHeapPath. The cost functions
/// (cost_index / cost_bitmap_heap_scan) let `add_path` pick the index when it is
/// cheaper than the seqscan.
pub fn create_index_paths(root: &mut PlannerInfo, rel: &mut RelOptInfo) {
    // The rel's index list is empty for a relation with no indexes (the common
    // case): nothing to do.
    if rel.indexlist.is_empty() {
        return;
    }

    // PG check_index_predicates fills each index's indrestrictinfo (the base
    // restriction clauses not implied by a partial-index predicate) before path
    // generation; M6 has no partial indexes, so it is the full baserestrictinfo.
    let baserestrict = rel.baserestrictinfo.clone();
    let relid = rel.relid as i32;
    let indexes = rel.indexlist.clone();

    for index in &indexes {
        let clauses = match_clauses_to_index(index, &baserestrict, relid);
        if clauses.is_empty() {
            continue; // no restriction clause uses this index
        }

        // The index's indrestrictinfo (M6: the full baserestrictinfo).
        let mut index = (**index).clone();
        index.indrestrictinfo.clone_from(&baserestrict);

        // A plain (non-index-only) IndexScan path.
        let ipath = create_index_path(root, rel, &index, clauses.clone(), ScanDirection::Forward);

        // A BitmapHeapPath over a BitmapIndexScan of the same index quals. PG always
        // also considers the bitmap form; the cheaper of the two (and of the seqscan)
        // wins in add_path.
        let bpath_index =
            create_index_path(root, rel, &index, clauses, ScanDirection::Forward);
        let bpath = create_bitmap_heap_path(root, rel, Box::new(bpath_index.path));

        add_path(rel, Box::new(ipath.path));
        add_path(rel, Box::new(bpath.path));
    }
}

/// PG `match_restriction_clauses_to_index` / `match_clause_to_index`: build the list
/// of `IndexClause`s for the restriction clauses that this index can check. Each
/// matched clause becomes one IndexClause naming the index column it uses.
fn match_clauses_to_index(
    index: &IndexOptInfo,
    clauses: &[Box<RestrictInfo>],
    relid: i32,
) -> Vec<Box<IndexClause>> {
    clauses
        .iter()
        .filter_map(|rinfo| match_clause_to_index(index, rinfo, relid))
        .collect()
}

/// PG `match_clause_to_index`: try to match one restriction clause against any index
/// column. Returns the `IndexClause` for the first matching column, or None.
fn match_clause_to_index(
    index: &IndexOptInfo,
    rinfo: &RestrictInfo,
    relid: i32,
) -> Option<Box<IndexClause>> {
    for indexcol in 0..index.nkeycolumns {
        if let Some(iclause) = match_clause_to_indexcol(index, rinfo, indexcol, relid) {
            return Some(iclause);
        }
    }
    None
}

/// PG `match_clause_to_indexcol`: does `rinfo`'s clause look like `indexkey op const`
/// where `indexkey` is index column `indexcol`? M6 matches a binary `OpExpr` whose
/// left operand is the base-rel Var of the index column's heap attribute and whose
/// right operand is a pseudoconstant, the operator being a btree strategy operator
/// of the column's opfamily. Returns the derived `IndexClause` (the indexqual is the
/// original clause with the index-col Var rewritten to INDEX_VAR by createplan).
fn match_clause_to_indexcol(
    index: &IndexOptInfo,
    rinfo: &RestrictInfo,
    indexcol: i32,
    relid: i32,
) -> Option<Box<IndexClause>> {
    let Node::OpExpr(op) = &rinfo.clause else {
        return None; // M6: only simple OpExpr restriction clauses
    };
    if op.args.len() != 2 {
        return None;
    }

    // The index column's heap attribute number (indexkeys is parallel to columns).
    let index_attno = *index.indexkeys.get(indexcol as usize)?;

    // leftop must be the base-rel Var of this index column; rightop a const.
    let Node::Var(var) = &op.args[0] else {
        return None;
    };
    if var.varno != relid || i32::from(var.varattno) != index_attno {
        return None;
    }
    if !is_pseudoconstant(&op.args[1]) {
        return None;
    }

    // The operator must be a btree strategy operator (M6 builtin int4 set).
    op_btree_strategy(op.opno)?;

    Some(Box::new(IndexClause {
        rinfo: Box::new(rinfo.clone()),
        // The indexqual is the same clause (createplan rewrites the index-col Var to
        // INDEX_VAR); a copy is kept so fix_indexqual can rewrite it without touching
        // the original clause used for the recheck (indexqualorig).
        indexquals: vec![Box::new(rinfo.clone())],
        lossy: false,
        indexcol: indexcol as i16,
        indexcols: Vec::new(),
    }))
}

/// Whether `node` is a constant the index can use as the comparison value. M6 covers
/// a plain `Const` (runtime Params / stable exprs grow with parameterized paths).
fn is_pseudoconstant(node: &Node) -> bool {
    matches!(node, Node::Const(_))
}

/// PG `get_op_opfamily_properties` (M6 subset): the btree strategy number of a
/// comparison operator, or None if it is not a known btree operator. Mirrors the
/// executor's `op_btree_strategy` (nodeIndexscan) but returns Option so a
/// non-indexable operator simply fails to match instead of panicking.
fn op_btree_strategy(opno: crate::postgres_ext::Oid) -> Option<StrategyNumber> {
    use crate::postgres_ext::Oid;
    let s = match opno {
        Oid(96) => BT_EQUAL_STRATEGY_NUMBER,          // int4 =
        Oid(97) => BT_LESS_STRATEGY_NUMBER,           // int4 <
        Oid(521) => BT_GREATER_STRATEGY_NUMBER,       // int4 >
        Oid(523) => BT_LESS_EQUAL_STRATEGY_NUMBER,    // int4 <=
        Oid(525) => BT_GREATER_EQUAL_STRATEGY_NUMBER, // int4 >=
        _ => return None,
    };
    Some(s)
}

/// Marker so INDEX_VAR stays referenced as the index-qual rewrite grows.
#[allow(dead_code)]
const fn _index_var_marker() -> i32 {
    INDEX_VAR
}

#[cfg(test)]
#[path = "indxpath_tests.rs"]
mod tests;
