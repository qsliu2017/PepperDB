//! The VACUUM / ANALYZE command driver. Translated from the M13-reachable core of
//! backend/commands/vacuum.c.
//!
//! M13 scope (step 46): `exec_vacuum` (the `VacuumStmt` entry from ProcessUtility),
//! `vacuum` (the driver over the target relation list), `vacuum_rel` (vacuum one
//! relation: open it under ShareUpdateExclusive, compute the dead-tuple cutoff
//! `OldestXmin`, run the lazy heap vacuum, update pg_class.relpages/reltuples), and
//! `get_all_vacuum_rels` (VACUUM with no table = every user table). ANALYZE routes
//! through `analyze_rel` (commands/analyze.rs). VACUUM FULL is STAGED to step 47.
//!
//! Async coloring (rules.md s5): the driver reaches the buffer pool / catalogs, so
//! it is `async`; no lock is held across `.await`.
//!
//! Staged (rules.md s4): VACUUM FULL / CLUSTER (step 47 -- surfaced as a clean
//! `not_yet_reachable`), TOAST-table vacuum, the two-phase transaction handling
//! (per-relation subtransactions), cost-based delay, autovacuum wiring, and the
//! wraparound-freeze failsafe. The plain lazy vacuum of a user heap + its indexes
//! is complete.

use std::sync::Arc;

use crate::backend::access::heap::vacuumlazy::lazy_scan_heap;
use crate::commands::vacuum::{VacOpt, VacuumParams};
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::VacuumStmt;
use crate::postgres_ext::{InvalidOid, Oid};
use crate::shared_state::SharedState;
use crate::utils::elog::{ERROR, WARNING};

/// `ExecVacuum`: the `VacuumStmt` entry from ProcessUtility. Parses the options
/// into `VacuumParams`, resolves the target relation list, and drives `vacuum`.
pub async fn exec_vacuum(shared: &Arc<SharedState>, stmt: &VacuumStmt) {
    let params = parse_vacuum_options(stmt);

    // A VacuumStmt is a VACUUM (is_vacuumcmd) or an ANALYZE; the options carry the
    // ANALYZE flag when `VACUUM ANALYZE` was written.
    let mut options = params.options;
    if stmt.is_vacuumcmd {
        options |= VacOpt::VACUUM;
    } else {
        options |= VacOpt::ANALYZE;
    }
    let params = VacuumParams { options, ..params };

    // VACUUM FULL routes to CLUSTER (step 47). Surface a clean staged error
    // (catchable ereport) rather than silently doing a plain vacuum.
    if options.contains(VacOpt::FULL) {
        crate::elog!(ERROR, "VACUUM FULL (rewrite via CLUSTER) is not yet supported -- step 47");
    }

    vacuum(shared, &stmt.rels, &params).await;
}

/// `vacuum`: drive VACUUM/ANALYZE over the target relation list. An empty list
/// means every vacuumable relation in the database (`get_all_vacuum_rels`).
pub async fn vacuum(shared: &Arc<SharedState>, rels: &[Node], params: &VacuumParams) {
    let targets = if rels.is_empty() {
        get_all_vacuum_rels(shared).await
    } else {
        let mut out = Vec::new();
        for node in rels {
            if let Some(t) = resolve_vacuum_relation(shared, node).await {
                out.push(t);
            }
        }
        out
    };

    for (relid, va_cols) in targets {
        if params.options.contains(VacOpt::VACUUM) {
            vacuum_rel(shared, relid, params).await;
        }
        if params.options.contains(VacOpt::ANALYZE) {
            crate::backend::commands::analyze::analyze_rel(shared, relid, params, &va_cols).await;
        }
    }
}

/// `vacuum_rel` (M13 lazy path): vacuum one relation `relid`. Opens it under
/// ShareUpdateExclusive (VACUUM's lock -- concurrent readers/writers allowed, only
/// other VACUUMs excluded), computes the dead-tuple cutoff `OldestXmin`, runs the
/// lazy heap vacuum over the heap + its indexes, then updates
/// pg_class.relpages/reltuples (and the relcache copy the planner reads).
async fn vacuum_rel(shared: &Arc<SharedState>, relid: Oid, _params: &VacuumParams) {
    use crate::backend::catalog::indexing::relation_get_index_list;
    use crate::backend::utils::cache::relcache::{relation_close, relation_id_get_relation};

    // VACUUM takes ShareUpdateExclusive; the heavyweight lock acquisition needs a
    // PGPROC, which the single-process backend does not register until InitProcess
    // lands (postinit). Follow the established command convention and open through
    // the relcache directly (the lock is a no-op given the single-writer chassis).
    let Some(relation) = relation_id_get_relation(relid) else { return };

    // Only plain heaps are vacuumed here (matviews/toast staged).
    let relkind = relation.form().relkind;
    if relkind != crate::catalog::pg_class::RELKIND_RELATION {
        relation_close(relation);
        return;
    }

    // Compute the dead-tuple horizon: a tuple whose xmax committed and precedes
    // OldestXmin can be seen by no snapshot, so it is removable.
    let oldest_xmin = shared
        .proc_array()
        .get_oldest_non_removable_transaction_id(shared.variable_cache(), Some(&relation));

    // Open the heap's indexes (the registry-backed rd_indexlist).
    let indexes: Vec<Arc<crate::utils::rel::RelationData>> = relation_get_index_list(relid)
        .into_iter()
        .map(|ri| ri.index)
        .collect();

    let result = lazy_scan_heap(shared, &relation, &indexes, oldest_xmin).await;

    // Update pg_class.relpages/reltuples (+ the relcache copy the planner reads).
    vac_update_relstats(shared, &relation, result.num_pages, result.num_live_tuples).await;

    relation_close(relation);
}

/// `vac_update_relstats` (M13 subset): write `num_pages`/`num_tuples` into the
/// relation's pg_class row and the relcache copy, so `estimate_rel_size` sees the
/// post-vacuum size. Freeze-xid / all-visible updates are staged.
async fn vac_update_relstats(
    shared: &Arc<SharedState>,
    relation: &crate::utils::rel::RelationData,
    num_pages: crate::storage::block::BlockNumber,
    num_tuples: f64,
) {
    crate::backend::commands::analyze::update_pg_class_stats(
        shared,
        relation.rd_id,
        num_pages as i32,
        num_tuples as f32,
    )
    .await;
}

/// `get_all_vacuum_rels`: every user (non-catalog) heap relation, for a bare
/// `VACUUM` / `ANALYZE`. M13: scan pg_class for RELKIND_RELATION rows in a user
/// namespace. Returns `(relid, va_cols)` pairs (no per-column list for a bare
/// command).
async fn get_all_vacuum_rels(shared: &Arc<SharedState>) -> Vec<(Oid, Vec<Node>)> {
    use crate::backend::access::common::heaptuple::heap_getattr;
    use crate::backend::access::heap::heapam::{heap_beginscan, heap_endscan, heap_getnext};
    use crate::backend::utils::cache::relcache::{relation_close, relation_id_get_relation};
    use crate::access::sdir::ScanDirection;
    use crate::access::tableam::ScanOptions;
    use crate::catalog::pg_class::{self as c, RelationRelationId, RELKIND_RELATION};
    use crate::postgres::DatumGetChar;

    let Some(pg_class) = relation_id_get_relation(RelationRelationId) else { return Vec::new() };
    let desc = pg_class
        .rd_att
        .clone()
        .unwrap_or_else(|| unreachable!("pg_class has a descriptor"));
    let snap = crate::backend::utils::time::snapmgr::GetActiveSnapshot()
        .unwrap_or_else(|| unreachable!("active snapshot for get_all_vacuum_rels"));

    let mut scan = heap_beginscan(&pg_class, &snap, 0, ScanOptions::ALLOW_PAGEMODE);
    let mut out = Vec::new();
    while let Some(tup) = heap_getnext(shared, &mut scan, ScanDirection::Forward).await {
        // SAFETY: live scan tuple over the pinned page.
        let tref = unsafe { &*tup };
        let (relkind, _) = unsafe { heap_getattr(tref, c::Anum_pg_class_relkind, &desc) };
        if DatumGetChar(relkind) as u8 != RELKIND_RELATION as u8 {
            continue;
        }
        let (namespace, _) = unsafe { heap_getattr(tref, c::Anum_pg_class_relnamespace, &desc) };
        let nsp = crate::postgres::DatumGetObjectId(namespace);
        // Skip catalog relations (pg_catalog / toast / information_schema).
        if is_system_namespace(nsp) {
            continue;
        }
        let (oidv, _) = unsafe { heap_getattr(tref, c::Anum_pg_class_oid, &desc) };
        out.push((crate::postgres::DatumGetObjectId(oidv), Vec::new()));
    }
    heap_endscan(shared, &mut scan);
    relation_close(pg_class);
    out
}

/// Whether `nsp` is a system namespace whose relations a bare VACUUM skips.
fn is_system_namespace(nsp: Oid) -> bool {
    use crate::catalog::pg_namespace::{PG_CATALOG_NAMESPACE, PG_TOAST_NAMESPACE};
    nsp == PG_CATALOG_NAMESPACE || nsp == PG_TOAST_NAMESPACE
}

/// Resolve one `VacuumRelation` node to `(relid, va_cols)`, looking up the relation
/// name in the catalog (PG `vacuum_open_relation` resolves the RangeVar). A pre-set
/// OID (from an earlier resolution) is used as-is; a name that does not resolve is a
/// WARNING-and-skip.
async fn resolve_vacuum_relation(shared: &Arc<SharedState>, node: &Node) -> Option<(Oid, Vec<Node>)> {
    let Node::VacuumRelation(vr) = node else { return None };
    if vr.oid != InvalidOid {
        return Some((vr.oid, vr.va_cols.clone()));
    }
    let relation = vr.relation.as_deref()?;
    let relname = relation.relname.as_deref()?;
    let relid = crate::backend::catalog::namespace::range_var_get_relid(
        shared,
        relation.schemaname.as_deref(),
        relname,
    )
    .await;
    match relid {
        Some(oid) if oid != InvalidOid => Some((oid, vr.va_cols.clone())),
        _ => {
            crate::elog!(WARNING, format!("skipping vacuum: relation \"{relname}\" does not exist"));
            None
        }
    }
}

/// Parse the `VacuumStmt` option `DefElem`s into a `VacuumParams`. Recognized:
/// `full`, `freeze`, `verbose`, `analyze` (and their `= true/false` args).
fn parse_vacuum_options(stmt: &VacuumStmt) -> VacuumParams {
    let mut options = VacOpt::empty();
    for opt in &stmt.options {
        let Node::DefElem(de) = opt else { continue };
        let Some(name) = de.defname.as_deref() else { continue };
        let on = defelem_bool(de);
        let flag = match name {
            "full" => VacOpt::FULL,
            "freeze" => VacOpt::FREEZE,
            "verbose" => VacOpt::VERBOSE,
            "analyze" => VacOpt::ANALYZE,
            _ => continue,
        };
        if on {
            options |= flag;
        }
    }
    VacuumParams {
        options,
        freeze_min_age: -1,
        freeze_table_age: -1,
        multixact_freeze_min_age: -1,
        multixact_freeze_table_age: -1,
        is_wraparound: false,
        log_min_duration: -1,
        index_cleanup: crate::commands::vacuum::VacOptValue::Unspecified,
        truncate: crate::commands::vacuum::VacOptValue::Unspecified,
        toast_parent: InvalidOid,
        max_eager_freeze_failure_rate: 0.0,
        nworkers: 0,
    }
}

/// Interpret a boolean option `DefElem`: `NONE` arg means "on" (the flag was
/// present, e.g. `VACUUM (FULL) t`); a string arg parses as a boolean.
fn defelem_bool(de: &crate::nodes::parsenodes::DefElem) -> bool {
    match &de.arg {
        Some(Node::String_(s)) => !matches!(s.sval.as_str(), "false" | "off" | "0" | "f"),
        // No arg (bare flag present) or a non-string arg: treat as "on".
        _ => true,
    }
}
