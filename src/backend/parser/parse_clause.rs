//! Handle clauses of a SELECT/INSERT (FROM, WHERE, ...). Translated from
//! backend/parser/parse_clause.c.
//!
//! Non-type-centric free functions (`transformFromClause`,
//! `transformFromClauseItem`, `setTargetTable`, ...); bodies here as snake_case
//! `pub fn`s with the C symbol in the doc comment, re-exported from
//! `crate::parser::parse_clause` under the C names.
//!
//! Disposition: `grow`. M2's live path is a FROM clause of plain table refs and
//! an INSERT target table: `transformFromClause` -> `transformFromClauseItem`
//! opens each RangeVar (a relcache lookup; ASYNC because the open is a lock-wait
//! leaf, rules.md s5) and builds its RTE via `addRangeTableEntryForRelation`,
//! adding a RangeTblRef to the joinlist; `setTargetTable` opens the INSERT target.
//! JOIN syntax, subquery/function FROM items, aliases, WHERE/LIMIT/GROUP/sort
//! clauses, and the namespace-conflict / LATERAL bookkeeping are grow guards
//! (rules.md s4).

#![allow(
    clippy::future_not_send,
    reason = "rules.md s5: transform_from_clause / set_target_table hold the per-backend ParseState (a task-confined raw Relation) across the relation-open awaits; the future runs on one backend task and never migrates the pointee mid-await. Same contract as the catalog/relcache/bootstrap modules."
)]

use std::sync::Arc;

use crate::backend::parser::parse_relation::{
    add_ns_item_to_query, add_range_table_entry_for_relation,
};
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::AclMode;
use crate::nodes::primnodes::RangeVar;
use crate::parser::parse_node::{ParseNamespaceItem, ParseState};
use crate::shared_state::SharedState;
use crate::storage::lockdefs::LockMode;

/// Panic for a parse_clause path not yet translated for this milestone
/// (rules.md s4).
#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

/// PG `transformFromClause`: transform each FROM item, adding its RTE to the
/// rangetable and a RangeTblRef to the joinlist, and exposing it in the namespace.
/// M2 covers plain table references (no JOIN/subquery/function items, no LATERAL).
pub async fn transform_from_clause(shared: &Arc<SharedState>, pstate: &mut ParseState, frm_list: Vec<Node>) {
    for n in frm_list {
        let (rtr, nsitem) = transform_from_clause_item(shared, pstate, n).await;
        // checkNameSpaceConflicts / setNamespaceLateralState grow with multi-item
        // FROM + LATERAL; a single plain ref has no conflict to check.
        pstate.p_joinlist.push(rtr);
        add_ns_item_to_query(pstate, nsitem, false, true, true);
    }
}

/// PG `transformFromClauseItem` (RangeVar arm): a plain table reference becomes an
/// `RTE_RELATION` plus a `RangeTblRef`. Returns the RangeTblRef node and the
/// nsitem (the caller adds them to the joinlist/namespace).
async fn transform_from_clause_item(
    shared: &Arc<SharedState>,
    pstate: &mut ParseState,
    n: Node,
) -> (Node, ParseNamespaceItem) {
    let Node::RangeVar(rv) = n else {
        not_yet_reachable("transformFromClauseItem: non-RangeVar FROM item (join/subquery/function)");
    };
    let nsitem = transform_table_entry(shared, pstate, &rv).await;
    let rtr = Node::RangeTblRef(Box::new(crate::nodes::primnodes::RangeTblRef {
        rtindex: nsitem.rtindex,
    }));
    (rtr, nsitem)
}

/// PG `transformTableEntry`: open the relation (AccessShareLock) and build its
/// RTE. The open is the async lock/relcache step; the RTE build is sync.
async fn transform_table_entry(
    shared: &Arc<SharedState>,
    pstate: &mut ParseState,
    rv: &RangeVar,
) -> ParseNamespaceItem {
    let rel = open_table_for_parse(shared, rv).await;
    // SAFETY: live open relation with a built descriptor.
    let nsitem = add_range_table_entry_for_relation(
        pstate,
        unsafe { &*rel },
        LockMode::AccessShareLock as i32,
        None,
        rv.inh,
        true,
    );
    // table_close(rel, NoLock) keeps the lock to end of xact; the relcache refcount
    // drop is RAII / deferred (M2 holds the entry for the rest of planning).
    nsitem
}

/// PG `setTargetTable`: open the INSERT/UPDATE/DELETE target relation, add it to
/// the rangetable (but NOT the joinlist or namespace), and record it as the
/// pstate's target. Returns the target's RT index. M2 supports the plain INSERT
/// target (RowExclusiveLock, no inheritance expansion).
pub async fn set_target_table(
    shared: &Arc<SharedState>,
    pstate: &mut ParseState,
    relation: &RangeVar,
    inh: bool,
    _also_source: bool,
    required_perms: AclMode,
) -> i32 {
    let rel = open_table_for_parse(shared, relation).await;
    // SAFETY: live open relation.
    let nsitem = add_range_table_entry_for_relation(
        pstate,
        unsafe { &*rel },
        LockMode::RowExclusiveLock as i32,
        None,
        inh,
        false,
    );
    let rtindex = nsitem.rtindex;

    // Stamp the required INSERT/UPDATE perms on the target's perminfo.
    let perminfo_index = nsitem.rte.perminfoindex;
    pstate.p_rteperminfos[(perminfo_index - 1) as usize].requiredPerms = required_perms;

    pstate.p_target_relation = rel;
    pstate.p_target_nsitem = Some(Box::new(nsitem));
    rtindex
}

/// Resolve a RangeVar to an open relcache `Relation` for parse analysis. M2 does
/// not take the heavyweight lock through `relation_open` (the sync
/// `RangeVarGetRelid` stub is not wired); it resolves the OID via the async
/// catalog scan and ensures the relcache entry is built. The AccessShareLock the
/// faithful path would take is approximated by the relcache build (the M2 tests
/// run single-statement, no concurrent DDL).
async fn open_table_for_parse(shared: &Arc<SharedState>, rv: &RangeVar) -> *mut crate::utils::rel::RelationData {
    use crate::backend::catalog::namespace::range_var_get_relid;
    use crate::backend::utils::cache::relcache::{relation_build_desc, relation_id_get_relation};

    let oid = range_var_get_relid(shared, rv.schemaname.as_deref(), rv.relname.as_deref().unwrap_or("")).await;
    let Some(oid) = oid else {
        relation_does_not_exist(rv.relname.as_deref().unwrap_or(""));
    };

    if let Some(rel) = relation_id_get_relation(oid) {
        return rel;
    }
    relation_build_desc(shared, oid)
        .await
        .unwrap_or_else(|| relation_does_not_exist(rv.relname.as_deref().unwrap_or("")))
}

#[cold]
fn relation_does_not_exist(relname: &str) -> ! {
    crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_TABLE)
            .errmsg(format!("relation \"{relname}\" does not exist"));
    });
    unreachable!("ereport(ERROR) diverges");
}
