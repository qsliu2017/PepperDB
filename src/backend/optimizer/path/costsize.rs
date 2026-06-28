//! Routines to compute (and set) relation and path costs. Translated from
//! backend/optimizer/path/costsize.c.
//!
//! Non-type-centric free functions; bodies here as snake_case `pub fn`s,
//! re-exported from `crate::optimizer::cost` under the C names.
//!
//! Disposition: `grow`. M2's live path is `cost_seqscan` for a plain base-rel
//! sequential scan: disk cost (`seq_page_cost * pages`) + CPU cost
//! (`cpu_tuple_cost * tuples` + tlist/qual eval). The cost GUCs use their compiled
//! defaults (the GUC subsystem is M9); parallelism, qual-cost evaluation of real
//! restriction clauses, and the many other `cost_*` estimators are grow guards
//! (rules.md s4).

use crate::nodes::pathnodes::{ParamPathInfo, Path, PlannerInfo, RelOptInfo};
use crate::nodes::parsenodes::RTEKind;
use crate::optimizer::cost::{DEFAULT_CPU_TUPLE_COST, DEFAULT_SEQ_PAGE_COST};

/// PG `cost_seqscan`: set `path`'s startup/total cost and row estimate for a
/// sequential scan of `baserel`. M2 uses the default tablespace page cost and the
/// default per-tuple CPU cost; there are no restriction quals (no WHERE yet), so
/// the qual cost is zero. Parallelism is not considered.
pub fn cost_seqscan(
    path: &mut Path,
    _root: &mut PlannerInfo,
    baserel: &RelOptInfo,
    param_info: Option<&ParamPathInfo>,
) {
    // Should only be applied to base relations.
    crate::assert!(baserel.relid > 0);
    crate::assert!(baserel.rtekind == RTEKind::RELATION);

    if param_info.is_some() {
        not_yet_reachable("cost_seqscan: parameterized path (ppi_rows)");
    }
    path.rows = baserel.rows;

    // Disk costs: seq_page_cost * pages (default tablespace page cost).
    let disk_run_cost = DEFAULT_SEQ_PAGE_COST * f64::from(baserel.pages);

    // CPU costs: no restriction quals in M2, so qpqual cost is zero.
    let mut startup_cost = 0.0;
    let cpu_per_tuple = DEFAULT_CPU_TUPLE_COST;
    let mut cpu_run_cost = cpu_per_tuple * baserel.tuples;

    // tlist eval costs are paid per output row.
    let target = path
        .pathtarget
        .as_ref()
        .unwrap_or_else(|| not_yet_reachable("cost_seqscan: missing pathtarget"));
    startup_cost += target.cost.startup;
    cpu_run_cost = target.cost.per_tuple.mul_add(path.rows, cpu_run_cost);

    if path.parallel_workers > 0 {
        not_yet_reachable("cost_seqscan: parallel sequential scan");
    }

    // disabled_nodes: enable_seqscan defaults true, so 0.
    path.disabled_nodes = 0;
    path.startup_cost = startup_cost;
    path.total_cost = startup_cost + cpu_run_cost + disk_run_cost;
}

#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}
