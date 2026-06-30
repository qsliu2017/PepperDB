//! Pre-planning target-list manipulation. Translated from
//! backend/optimizer/prep/preptlist.c.
//!
//! Non-type-centric free functions; bodies here as snake_case `pub fn`s,
//! re-exported from `crate::optimizer::prep` under the C names.
//!
//! Disposition: `grow`. `preprocess_targetlist` expands/orders the query's
//! target list into `root->processed_tlist`. For a SELECT (M1's only command)
//! there is no result relation and nothing to expand: it simply adopts the
//! query's target list. The INSERT column-fill, UPDATE colno extraction,
//! UPDATE/DELETE/MERGE row-identity junk columns, and FOR UPDATE rowmark junk
//! columns all route through a single grow guard (rules.md s4) and grow with the
//! DML milestones.

use crate::nodes::nodes::{CmdType, Node};
use crate::nodes::pathnodes::PlannerInfo;

/// Panic for a target-list path not yet translated for this milestone
/// (rules.md s4).
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

/// PG `preprocess_targetlist`: prepare the parse tree target list for planning,
/// storing the result in `root->processed_tlist`.
///
/// M8 (step 34): for UPDATE the SET tlist is expanded so every result-relation
/// column has a tlist entry (the unchanged columns get a `Var` reading their old
/// value), ordered by attno -- the subplan slot is thus the complete new tuple. For
/// DELETE the tlist is emptied (the row identity is the scan slot's TID, read by the
/// executor; PG carries a `ctid` junk Var which the system-column-Var executor path
/// would project -- staged, the slot's `tts_tid` carries the identity directly).
pub fn preprocess_targetlist(root: &mut PlannerInfo) {
    let command_type = root.parse.commandType;
    let result_relation = root.parse.resultRelation;

    if result_relation == 0 {
        crate::assert!(command_type == CmdType::SELECT);
    }

    let tlist = match command_type {
        // SELECT / MERGE: the tlist passes through (MERGE execution is staged).
        CmdType::SELECT | CmdType::MERGE => root.parse.targetList.clone(),
        // INSERT: expand to a complete attno-ordered tlist. A column named in the
        // INSERT was keyed to its attno by transformInsertStmt; an omitted column is
        // filled with its SET DEFAULT expression (pg_attrdef, via the relcache
        // descriptor's TupleConstr.defval) or a NULL of the column type. This is PG's
        // expand_targetlist INSERT path (the rewriter's rewriteTargetListIns +
        // build_column_default analog).
        CmdType::INSERT => expand_insert_targetlist(root, result_relation),
        // UPDATE: expand the SET tlist to a complete attno-ordered new tuple.
        CmdType::UPDATE => expand_update_targetlist(root, result_relation),
        // DELETE: no new tuple is needed (the TID identifies the row), so the scan
        // projects nothing -- UNLESS there is a RETURNING list, which references the
        // deleted row's columns; then the scan must project the full old row.
        CmdType::DELETE => {
            if root.parse.returningList.is_empty() {
                Vec::new()
            } else {
                expand_delete_targetlist(root, result_relation)
            }
        }
        other => not_yet_reachable(&format!("preprocess_targetlist: command type {other:?}")),
    };

    // FOR UPDATE/SHARE adds junk TID/whole-row columns in PG. The LockRows executor
    // reads the row identity from the scan slot's TID (the system-column-Var junk
    // path is staged), so no junk column is appended; the rowmarks drive ExecLockRows
    // directly. Nothing to add to the tlist here.

    root.processed_tlist = tlist;
}

/// PG `expand_targetlist` (UPDATE subset): produce a target list with one entry per
/// live column of the result relation, in attno order. A column named in the SET
/// list takes its assigned expression; an unchanged column gets a `Var` reading its
/// current value (so the new tuple is complete). The result relation's tuple
/// descriptor comes from the planner's relcache info (`get_relation_info`), here read
/// from the simple_rel_array entry / the rangetable's relid via the relcache.
fn expand_update_targetlist(root: &PlannerInfo, result_relation: i32) -> Vec<Node> {
    expand_result_rel_targetlist(root, result_relation, true)
}

/// PG `expand_targetlist` (DELETE-with-RETURNING subset): project the full old row of
/// the result relation (a `Var` per column) so the RETURNING list can read it.
fn expand_delete_targetlist(root: &PlannerInfo, result_relation: i32) -> Vec<Node> {
    expand_result_rel_targetlist(root, result_relation, false)
}

/// PG `expand_targetlist` (INSERT path): produce an attno-ordered tlist with one
/// entry per live column of the result relation. A column the INSERT named keeps its
/// assigned expression; an omitted column gets its SET DEFAULT expression (from the
/// relcache descriptor's `TupleConstr.defval`, populated from pg_attrdef) or a NULL
/// constant of the column type.
fn expand_insert_targetlist(root: &PlannerInfo, result_relation: i32) -> Vec<Node> {
    use crate::nodes::makefuncs::makeTargetEntry;
    use crate::nodes::primnodes::TargetEntry;

    let Node::RangeTblEntry(rte) = &root.parse.rtable[(result_relation - 1) as usize] else {
        not_yet_reachable("expand_insert_targetlist: result RTE is not an RTE");
    };
    let rel = crate::backend::utils::cache::relcache::relation_id_get_relation(rte.relid)
        .unwrap_or_else(|| not_yet_reachable("expand_insert_targetlist: result relation not in relcache"));
    let tupdesc = rel
        .rd_att
        .as_ref()
        .unwrap_or_else(|| not_yet_reachable("expand_insert_targetlist: result relation has no descriptor"));

    let natts = tupdesc.natts as usize;
    let result: Vec<Node> = (0..natts)
        .map(|i| {
            let attno = (i + 1) as crate::access::attnum::AttrNumber;
            let attr = tupdesc.attr(i);

            // A column the INSERT provided keeps its expression (keyed by resno).
            let provided = root.parse.targetList.iter().find_map(|n| match n {
                Node::TargetEntry(te) if !te.resjunk && te.resno == attno => te.expr.clone(),
                _ => None,
            });
            let expr = provided.unwrap_or_else(|| build_insert_default(tupdesc, attno, attr.atttypid));

            let resname = {
                let bytes = crate::c::NameStr(&attr.attname);
                let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
                String::from_utf8_lossy(&bytes[..end]).into_owned()
            };
            let tle: TargetEntry = makeTargetEntry(Some(expr), attno, Some(resname), false);
            Node::TargetEntry(Box::new(tle))
        })
        .collect();
    crate::backend::utils::cache::relcache::relation_close(rel);
    result
}

/// PG `build_column_default`: the default expression for a column omitted from an
/// INSERT. Reads the descriptor's `TupleConstr.defval` (pg_attrdef, the deparsed
/// default text); a literal-integer default becomes its int4 `Const` (the reachable
/// SET DEFAULT form -- the test's `SET DEFAULT 5`). A column with no default, or a
/// non-literal default, becomes a NULL constant of the column type (PG fills NULL
/// for a defaultless column; the general cooked-default-expression store stages).
fn build_insert_default(
    tupdesc: &crate::access::tupdesc::TupleDescData,
    attno: crate::access::attnum::AttrNumber,
    atttypid: crate::postgres_ext::Oid,
) -> Node {
    use crate::backend::nodes::makefuncs::make_const;
    use crate::postgres::{Datum, Int32GetDatum};

    let default_text = tupdesc
        .constr
        .as_ref()
        .and_then(|c| c.defval.iter().find(|d| d.adnum == attno))
        .map(|d| d.adbin.as_str());

    if let Some(text) = default_text {
        // The deparsed default. A bare integer literal -> an int4 Const (reachable
        // SET DEFAULT). Anything else stages to NULL (general cooked-default store).
        if let Ok(ival) = text.trim().parse::<i32>() {
            let con = make_const(
                crate::catalog::genbki::INT4OID,
                -1,
                crate::postgres_ext::InvalidOid,
                4,
                Int32GetDatum(ival),
                false,
                true,
            );
            return Node::Const(Box::new(con));
        }
    }

    // Defaultless (or non-literal default): a NULL const of the column type.
    let con = make_const(atttypid, -1, crate::postgres_ext::InvalidOid, -1, Datum(0), true, false);
    Node::Const(Box::new(con))
}

/// Build an attno-ordered tlist over the result relation's columns. When
/// `apply_set` is true (UPDATE), a column named in the parsed SET tlist takes its
/// assigned expression; otherwise (DELETE-with-RETURNING) every column is a plain
/// `Var` reading its current value.
fn expand_result_rel_targetlist(
    root: &PlannerInfo,
    result_relation: i32,
    apply_set: bool,
) -> Vec<Node> {
    use crate::nodes::makefuncs::{makeTargetEntry, makeVar};
    use crate::nodes::primnodes::TargetEntry;

    let Node::RangeTblEntry(rte) = &root.parse.rtable[(result_relation - 1) as usize] else {
        not_yet_reachable("expand_targetlist: result RTE is not an RTE");
    };
    let rel = crate::backend::utils::cache::relcache::relation_id_get_relation(rte.relid)
        .unwrap_or_else(|| not_yet_reachable("expand_targetlist: result relation not in relcache"));
    let tupdesc = rel
        .rd_att
        .as_ref()
        .unwrap_or_else(|| not_yet_reachable("expand_targetlist: result relation has no descriptor"));

    let natts = tupdesc.natts as usize;
    let result: Vec<Node> = (0..natts)
        .map(|i| {
            let attno = (i + 1) as crate::access::attnum::AttrNumber;
            let attr = tupdesc.attr(i);
            let assigned = apply_set
                .then(|| {
                    root.parse.targetList.iter().find_map(|n| match n {
                        Node::TargetEntry(te) if !te.resjunk && te.resno == attno => te.expr.clone(),
                        _ => None,
                    })
                })
                .flatten();
            let expr = assigned.unwrap_or_else(|| {
                // A Var over the result relation reading the column's current value.
                Node::Var(Box::new(makeVar(
                    result_relation,
                    attno,
                    attr.atttypid,
                    attr.atttypmod,
                    attr.attcollation,
                    0,
                )))
            });
            let resname = {
                let bytes = crate::c::NameStr(&attr.attname);
                let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
                String::from_utf8_lossy(&bytes[..end]).into_owned()
            };
            let tle: TargetEntry = makeTargetEntry(Some(expr), attno, Some(resname), false);
            Node::TargetEntry(Box::new(tle))
        })
        .collect();
    crate::backend::utils::cache::relcache::relation_close(rel);
    result
}
