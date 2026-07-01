//! Selectivity functions for standard operators. Translated from
//! backend/utils/adt/selfuncs.c (disposition: grow).
//!
//! These are the per-operator restriction (`oprrest`) and join (`oprjoin`)
//! selectivity estimators. PG reads `pg_statistic` (the ANALYZE stats) to compute a
//! real estimate. As of M13 (step 46) the restriction estimators CONSUME the real
//! stats: `examine_variable` resolves the clause's Var to its base relation column,
//! reads that column's stats from the process-global stats cache ANALYZE publishes
//! (see the cache section below), and:
//!   - `eqsel` uses the MCV frequency for an MCV constant, else
//!     `(1 - sum(MCV) - nullfrac) / (ndistinct - num_MCV)`.
//!   - `scalar{lt,gt,le,ge}sel` uses the histogram fraction below/above the
//!     constant, blended with the MCV mass on that side.
//!
//! When a column has no cached stats (un-analyzed relation), each estimator falls
//! back to the NO-STATISTICS DEFAULT path -- `eqsel -> 1/200 = DEFAULT_EQ_SEL`,
//! `scalar* -> DEFAULT_INEQ_SEL`, `eqjoinsel -> 1/max(nd) = 1/200` -- so
//! un-analyzed tables keep the M7 behavior. The join estimators still take the
//! no-stats default (join-side stats consumption is a later step).
//!
//! The estimators are bound to operators through the seeded `pg_operator.oprrest` /
//! `oprjoin` columns: `clause_selectivity` looks up an operator's `oprrest` proc OID
//! (`get_oprrest`) and dispatches to `restriction_selectivity`; the join path uses
//! `get_oprjoin` + `join_selectivity`.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use crate::nodes::nodes::{JoinType, Node, Selectivity};
use crate::nodes::pathnodes::{PlannerInfo, SpecialJoinInfo};
use crate::postgres::{DatumGetInt16, DatumGetInt32};
use crate::postgres_ext::Oid;

// ---------------------------------------------------------------------------
// pg_statistic consumption (M13, step 46)
// ---------------------------------------------------------------------------
//
// The planner runs synchronously and does not thread `SharedState`, so it cannot
// heap-scan pg_statistic during costing (and the sync catcache holds only warm
// entries, which are per-task). ANALYZE therefore publishes each column's computed
// statistics into this process-global cache alongside writing the durable
// pg_statistic rows; `examine_variable` reads the cache here. The durable
// pg_statistic rows remain the source of truth (their existence is what the ANALYZE
// test asserts); this cache is the planner's fast read path, playing the role PG's
// syscache plays for the (async-populated) stats tuple.

/// One column's cached statistics for the planner (the subset selfuncs consumes:
/// ndistinct, the MCV values+frequencies, and the histogram bounds). MCV values and
/// histogram bounds are widened `i64` scalars (the M2 heap's sortable columns are
/// int2/int4/int8); the query constant is widened the same way to compare.
#[derive(Debug, Clone, Default)]
pub struct CachedColumnStats {
    pub stanullfrac: f32,
    pub stadistinct: f32,
    pub mcv_values: Vec<i64>,
    pub mcv_freqs: Vec<f32>,
    pub histogram: Vec<i64>,
}

/// The process-global stats cache, keyed by `(relid, attnum)`.
static STATS_CACHE: OnceLock<Mutex<HashMap<(u32, i16), CachedColumnStats>>> = OnceLock::new();

fn stats_cache() -> &'static Mutex<HashMap<(u32, i16), CachedColumnStats>> {
    STATS_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Publish one column's statistics (called by ANALYZE after writing pg_statistic).
/// Accepts the analyze-side `ColumnStats` and stores the planner-consumed subset.
pub fn store_column_stats(relid: Oid, stats: &crate::backend::commands::analyze::ColumnStats) {
    let cached = CachedColumnStats {
        stanullfrac: stats.stanullfrac,
        stadistinct: stats.stadistinct,
        mcv_values: stats.mcv_values.clone(),
        mcv_freqs: stats.mcv_freqs.clone(),
        histogram: stats.histogram.clone(),
    };
    if let Ok(mut map) = stats_cache().lock() {
        map.insert((relid.get(), stats.attnum), cached);
    }
}

/// Look up cached statistics for `(relid, attnum)`, if ANALYZE has run.
fn lookup_column_stats(relid: Oid, attnum: i16) -> Option<CachedColumnStats> {
    stats_cache().lock().ok()?.get(&(relid.get(), attnum)).cloned()
}

/// The variable + constant a restriction clause references, as `examine_variable`
/// resolves it: the base relation OID + column, the column's cached stats (if
/// analyzed), and the (widened) constant scalar when the other side is a Const.
struct ExaminedVariable {
    stats: Option<CachedColumnStats>,
    const_scalar: Option<i64>,
}

/// PG `examine_variable` (M13 subset): from a binary operator's `args`, find the Var
/// side, resolve its base relation OID + column via `root`'s range table, load its
/// cached pg_statistic stats, and widen the Const side (if present) to an i64 for
/// comparison. Returns `None` if neither side is a simple Var over a base relation.
fn examine_variable(root: &PlannerInfo, args: &[Node]) -> Option<ExaminedVariable> {
    let mut var: Option<&crate::nodes::primnodes::Var> = None;
    let mut cnst: Option<&crate::nodes::primnodes::Const> = None;
    for a in args {
        match a {
            Node::Var(v) => var = Some(v),
            Node::Const(c) => cnst = Some(c),
            _ => {}
        }
    }
    let var = var?;

    // Resolve varno -> base relation OID via the range table.
    let idx = var.varno as usize;
    let relid = root
        .simple_rte_array
        .get(idx)
        .and_then(|o| o.as_ref())
        .map(|rte| rte.relid)?;
    if !relid.is_valid() || var.varattno <= 0 {
        return None;
    }

    let stats = lookup_column_stats(relid, var.varattno);
    let const_scalar = cnst.and_then(|c| {
        (!c.constisnull).then(|| widen_const(c.consttype, c.constvalue))
    });
    Some(ExaminedVariable { stats, const_scalar })
}

/// Widen a by-value scalar Const to i64 (matching ANALYZE's `widen_scalar`).
fn widen_const(typid: Oid, val: crate::postgres::Datum) -> i64 {
    match typid.get() {
        21 => i64::from(DatumGetInt16(val)),
        23 => i64::from(DatumGetInt32(val)),
        // int8 (20) and any other by-value scalar: the low 64 bits.
        _ => val.0 as i64,
    }
}

/// Test hook: clear the process-global stats cache (tests share the process, so a
/// prior test's ANALYZE can leave entries whose `relid` a later test reuses).
#[cfg(test)]
pub fn clear_stats_cache_for_test() {
    if let Ok(mut map) = stats_cache().lock() {
        map.clear();
    }
}

/// Test hook: the equality selectivity `eqsel` would compute for `(relid, attnum)`
/// against the integer constant `k`, exercising the real stats path (or the no-stats
/// default when `(relid, attnum)` has no cached stats).
#[cfg(test)]
pub fn eqsel_for_test(relid: Oid, attnum: i16, k: i64) -> Selectivity {
    lookup_column_stats(relid, attnum).map_or_else(
        || clamp_probability(1.0 / get_variable_numdistinct()),
        |stats| clamp_probability(eqsel_with_stats(&stats, Some(k))),
    )
}

/// Test hook: read `reltuples` from the durable pg_class row for `relid` (the
/// milestone's "pg_class.reltuples ~ N" assertion). Runs a catalog scan; the caller
/// must be inside a read transaction with an active snapshot.
#[cfg(test)]
#[allow(
    clippy::cast_ptr_alignment,
    reason = "GETSTRUCT reinterpretation of a MAXALIGN'd heap tuple to Form_pg_class"
)]
pub async fn pg_class_reltuples_for_test(
    shared: &std::sync::Arc<crate::shared_state::SharedState>,
    relid: Oid,
) -> f64 {
    use crate::access::htup_details::GETSTRUCT;
    use crate::catalog::pg_class::{self as pc, FormData_pg_class, RelationRelationId};

    let rows = crate::backend::commands::tablecmds::scan_catalog_rows_by_oid(
        shared,
        RelationRelationId,
        pc::Anum_pg_class_oid,
        relid,
    )
    .await;
    let mut out = -1.0;
    for row in &rows {
        // SAFETY: owned tuple; fixed part starts with FormData_pg_class.
        let p = GETSTRUCT(&row.tuple).cast::<FormData_pg_class>();
        if unsafe { (*p).oid } == relid {
            out = f64::from(unsafe { (*p).reltuples });
            break;
        }
    }
    for row in rows {
        crate::backend::access::common::heaptuple::heap_freetuple(row.tuple);
    }
    out
}

// selfuncs.h default-selectivity constants.
/// `DEFAULT_EQ_SEL`: default selectivity for "A = B".
pub const DEFAULT_EQ_SEL: Selectivity = 0.005;
/// `DEFAULT_INEQ_SEL`: default selectivity for "A < B", "A > B", etc.
pub const DEFAULT_INEQ_SEL: Selectivity = 0.333_333_333_333_333_3;
/// `DEFAULT_NUM_DISTINCT`: default number of distinct values for a column with no
/// statistics. Chosen so `1 / DEFAULT_NUM_DISTINCT == DEFAULT_EQ_SEL`.
pub const DEFAULT_NUM_DISTINCT: f64 = 200.0;

// pg_proc OIDs of the selectivity estimators (pg_proc.dat), used as the `oprrest`/
// `oprjoin` discriminants the seeded pg_operator rows point at.
/// `eqsel` restriction estimator proc OID.
pub const F_EQSEL: u32 = 101;
/// `neqsel` restriction estimator proc OID.
pub const F_NEQSEL: u32 = 102;
/// `scalarltsel` restriction estimator proc OID.
pub const F_SCALARLTSEL: u32 = 103;
/// `scalargtsel` restriction estimator proc OID.
pub const F_SCALARGTSEL: u32 = 104;
/// `eqjoinsel` join estimator proc OID.
pub const F_EQJOINSEL: u32 = 105;
/// `neqjoinsel` join estimator proc OID.
pub const F_NEQJOINSEL: u32 = 106;
/// `scalarltjoinsel` join estimator proc OID.
pub const F_SCALARLTJOINSEL: u32 = 107;
/// `scalargtjoinsel` join estimator proc OID.
pub const F_SCALARGTJOINSEL: u32 = 108;
/// `scalarlesel` restriction estimator proc OID.
pub const F_SCALARLESEL: u32 = 336;
/// `scalargesel` restriction estimator proc OID.
pub const F_SCALARGESEL: u32 = 337;
/// `scalarlejoinsel` join estimator proc OID.
pub const F_SCALARLEJOINSEL: u32 = 386;
/// `scalargejoinsel` join estimator proc OID.
pub const F_SCALARGEJOINSEL: u32 = 398;

/// PG `clamp` of a probability into `[0, 1]` (`CLAMP_PROBABILITY`).
fn clamp_probability(p: Selectivity) -> Selectivity {
    p.clamp(0.0, 1.0)
}

/// PG `get_variable_numdistinct` (no-stats path): the number of distinct values to
/// assume for a column with no `pg_statistic` row. PG falls back to
/// `DEFAULT_NUM_DISTINCT` when there is no stats tuple and the table size is
/// unknown / large; the only reachable path here (no ANALYZE) returns the default.
fn get_variable_numdistinct() -> f64 {
    DEFAULT_NUM_DISTINCT
}

/// PG `eqsel` / `eqsel_internal(negate=false)`: equality restriction selectivity
/// ("var = const").
///
/// With stats (post-ANALYZE): if the constant is a most-common value, its recorded
/// frequency; otherwise `(1 - sum(MCV_freqs)) / (ndistinct - num_MCV)` -- the
/// even spread over the non-MCV distinct values. With no stats (un-analyzed table),
/// the no-stats path `1 / get_variable_numdistinct` = 1/200 = `DEFAULT_EQ_SEL` is
/// kept (no regression for un-analyzed tables).
pub fn eqsel(
    root: &mut PlannerInfo,
    _operator: Oid,
    args: &[Node],
    _var_relid: i32,
) -> Selectivity {
    if let Some(ev) = examine_variable(root, args)
        && let Some(stats) = ev.stats
    {
        return clamp_probability(eqsel_with_stats(&stats, ev.const_scalar));
    }
    // No-stats fallback (un-analyzed table): 1 / DEFAULT_NUM_DISTINCT.
    clamp_probability(1.0 / get_variable_numdistinct())
}

/// Equality selectivity from a column's stats. `const_scalar` is the (widened) query
/// constant, or `None` if the other side is not a plain Const (then use the average
/// non-MCV frequency). PG `var_eq_const` core.
fn eqsel_with_stats(stats: &CachedColumnStats, const_scalar: Option<i64>) -> f64 {
    let sum_mcv: f64 = stats.mcv_freqs.iter().map(|&f| f64::from(f)).sum();

    // If the constant matches an MCV, return that MCV's frequency directly.
    if let Some(k) = const_scalar
        && let Some(pos) = stats.mcv_values.iter().position(|&v| v == k)
    {
        return f64::from(stats.mcv_freqs[pos]);
    }

    // Otherwise spread the non-MCV mass over the non-MCV distinct values.
    let ndistinct = effective_ndistinct(stats);
    let num_mcv = stats.mcv_values.len() as f64;
    let otherdistinct = (ndistinct - num_mcv).max(1.0);
    let selec = (1.0 - sum_mcv - f64::from(stats.stanullfrac)).max(0.0) / otherdistinct;
    selec.max(0.0)
}

/// The column's effective distinct-value count from `stadistinct` (a positive
/// count, or a negative multiplier of the row count -- here approximated by the
/// MCV+histogram cardinality when negative, since the planner-side row count is not
/// threaded into this cache).
fn effective_ndistinct(stats: &CachedColumnStats) -> f64 {
    if stats.stadistinct > 0.0 {
        f64::from(stats.stadistinct)
    } else {
        // Negative multiplier (all/nearly distinct): approximate the distinct count
        // by the observed MCV + histogram bound cardinality, falling back to the
        // default when neither is present.
        let observed = (stats.mcv_values.len() + stats.histogram.len()) as f64;
        if observed > 1.0 { observed } else { DEFAULT_NUM_DISTINCT }
    }
}

/// PG `neqsel` / `eqsel_internal(negate=true)`: "var <> const" restriction
/// selectivity, `1 - eqsel` (no-stats: `1 - DEFAULT_EQ_SEL`).
pub fn neqsel(
    root: &mut PlannerInfo,
    operator: Oid,
    args: &[Node],
    var_relid: i32,
) -> Selectivity {
    clamp_probability(1.0 - eqsel(root, operator, args, var_relid))
}

/// PG `scalarltsel`/`scalarlesel`/`scalargtsel`/`scalargesel`: an ordering-comparison
/// restriction. With a histogram (post-ANALYZE), the fraction of the column below
/// (`is_lt`) or above the constant, blended with the MCV mass on the matching side.
/// With no stats (un-analyzed), the `DEFAULT_INEQ_SEL` fallback is kept.
///
/// `is_lt` selects the `<`/`<=` (below) vs `>`/`>=` (above) direction.
fn scalarineqsel(root: &PlannerInfo, args: &[Node], is_lt: bool) -> Selectivity {
    if let Some(ev) = examine_variable(root, args)
        && let (Some(stats), Some(k)) = (ev.stats.as_ref(), ev.const_scalar)
        && !stats.histogram.is_empty()
    {
        return clamp_probability(scalarineqsel_with_hist(stats, k, is_lt));
    }
    DEFAULT_INEQ_SEL
}

/// The `scalarltsel` no-stats entry kept for the join-side dispatch and any caller
/// without a resolvable variable.
pub fn scalarineqsel_default(
    _root: &mut PlannerInfo,
    _operator: Oid,
    _args: &[Node],
    _var_relid: i32,
) -> Selectivity {
    DEFAULT_INEQ_SEL
}

/// Ordering-selectivity from the equi-depth histogram: the fraction of histogram
/// buckets whose bound is below `k` (for `<`/`<=`) or above (for `>`/`>=`), plus the
/// MCV frequencies on that side. Mirrors `ineq_histogram_selectivity`'s core (linear
/// interpolation omitted -- bucket granularity is sufficient for the M13 tests).
fn scalarineqsel_with_hist(stats: &CachedColumnStats, k: i64, is_lt: bool) -> f64 {
    let hist = &stats.histogram;
    let n = hist.len();
    // Position of k among the bounds: count bounds strictly below k.
    let below = hist.iter().filter(|&&b| b < k).count();
    // Histogram fraction below k (bounds are equi-depth bucket boundaries).
    let frac_below = below as f64 / (n as f64 - 1.0).max(1.0);
    let hist_sel = if is_lt { frac_below } else { 1.0 - frac_below };

    // Add the MCV mass on the matching side.
    let mcv_sel: f64 = stats
        .mcv_values
        .iter()
        .zip(&stats.mcv_freqs)
        .filter(|(v, _)| if is_lt { **v < k } else { **v > k })
        .map(|(_, &f)| f64::from(f))
        .sum();

    // Blend: the histogram covers the (1 - sum(MCV) - nullfrac) mass.
    let sum_mcv: f64 = stats.mcv_freqs.iter().map(|&f| f64::from(f)).sum();
    let hist_mass = (1.0 - sum_mcv - f64::from(stats.stanullfrac)).max(0.0);
    (hist_sel * hist_mass + mcv_sel).clamp(0.0, 1.0)
}

/// PG `eqjoinsel` / `eqjoinsel_inner` (no-stats, no-MCV path): equality JOIN
/// selectivity ("a.x = b.y"). With no stats on either side and both numdistinct
/// estimates defaulting to `DEFAULT_NUM_DISTINCT`, the formula
/// `(1 - nullfrac1)(1 - nullfrac2) / max(nd1, nd2)` reduces to `1 / max(nd1, nd2)`
/// = 1/200. This is the value that drives `calc_joinrel_size_estimate`.
pub fn eqjoinsel(
    _root: &mut PlannerInfo,
    _operator: Oid,
    _args: &[Node],
    _jointype: JoinType,
    _sjinfo: &SpecialJoinInfo,
) -> Selectivity {
    let nd1 = get_variable_numdistinct();
    let nd2 = get_variable_numdistinct();
    clamp_probability(1.0 / nd1.max(nd2))
}

/// PG `neqjoinsel` (no-stats path): `1 - eqjoinsel` for "<>"-style join clauses.
pub fn neqjoinsel(
    root: &mut PlannerInfo,
    operator: Oid,
    args: &[Node],
    jointype: JoinType,
    sjinfo: &SpecialJoinInfo,
) -> Selectivity {
    clamp_probability(1.0 - eqjoinsel(root, operator, args, jointype, sjinfo))
}

/// PG `scalar{lt,gt,le,ge}joinsel` (no-stats path): an ordering-comparison join
/// clause defaults to `DEFAULT_INEQ_SEL`.
pub fn scalarineqjoinsel_default(
    _root: &mut PlannerInfo,
    _operator: Oid,
    _args: &[Node],
    _jointype: JoinType,
    _sjinfo: &SpecialJoinInfo,
) -> Selectivity {
    DEFAULT_INEQ_SEL
}

/// Dispatch on an operator's `oprrest` proc OID to its restriction estimator. PG
/// calls the proc via the fmgr table; the port dispatches on the proc OID
/// (`get_oprrest`) directly. An operator with no restriction estimator
/// (`InvalidOid`) gets the generic `DEFAULT_EQ_SEL` (PG's `restriction_selectivity`
/// would error, but the seeded comparison/equality operators all carry an oprrest).
pub fn restriction_selectivity(
    root: &mut PlannerInfo,
    oprrest: Oid,
    operator: Oid,
    args: &[Node],
    var_relid: i32,
) -> Selectivity {
    match oprrest.get() {
        F_EQSEL => eqsel(root, operator, args, var_relid),
        F_NEQSEL => neqsel(root, operator, args, var_relid),
        F_SCALARLTSEL | F_SCALARLESEL => scalarineqsel(root, args, true),
        F_SCALARGTSEL | F_SCALARGESEL => scalarineqsel(root, args, false),
        _ => DEFAULT_EQ_SEL,
    }
}

/// Dispatch on an operator's `oprjoin` proc OID to its join estimator (PG
/// `join_selectivity`).
pub fn join_selectivity(
    root: &mut PlannerInfo,
    oprjoin: Oid,
    operator: Oid,
    args: &[Node],
    jointype: JoinType,
    sjinfo: &SpecialJoinInfo,
) -> Selectivity {
    match oprjoin.get() {
        F_EQJOINSEL => eqjoinsel(root, operator, args, jointype, sjinfo),
        F_NEQJOINSEL => neqjoinsel(root, operator, args, jointype, sjinfo),
        F_SCALARLTJOINSEL | F_SCALARGTJOINSEL | F_SCALARLEJOINSEL | F_SCALARGEJOINSEL => {
            scalarineqjoinsel_default(root, operator, args, jointype, sjinfo)
        }
        _ => DEFAULT_EQ_SEL,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres_ext::{InvalidOid, Oid};

    fn root() -> PlannerInfo {
        crate::backend::optimizer::plan::initsplan::tests::test_planner_info()
    }

    fn sjinfo() -> SpecialJoinInfo {
        SpecialJoinInfo {
            min_lefthand: None,
            min_righthand: None,
            syn_lefthand: None,
            syn_righthand: None,
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

    #[test]
    fn eqsel_no_stats_is_default_eq_sel() {
        let mut root = root();
        let s = eqsel(&mut root, Oid::new(96), &[], 1);
        assert!((s - DEFAULT_EQ_SEL).abs() < 1e-12, "eqsel no-stats = 1/200 = {DEFAULT_EQ_SEL}");
    }

    #[test]
    fn neqsel_no_stats_is_one_minus_default() {
        let mut root = root();
        let s = neqsel(&mut root, Oid::new(96), &[], 1);
        assert!((s - (1.0 - DEFAULT_EQ_SEL)).abs() < 1e-12);
    }

    #[test]
    fn scalarltsel_no_stats_is_default_ineq() {
        let mut root = root();
        let s = scalarineqsel_default(&mut root, Oid::new(97), &[], 1);
        assert!((s - DEFAULT_INEQ_SEL).abs() < 1e-12);
    }

    #[test]
    fn eqjoinsel_no_stats_is_one_over_max_nd() {
        let mut root = root();
        let sj = sjinfo();
        let s = eqjoinsel(&mut root, Oid::new(96), &[], JoinType::INNER, &sj);
        // 1 / max(200, 200) = 1/200 = DEFAULT_EQ_SEL.
        assert!((s - 1.0 / DEFAULT_NUM_DISTINCT).abs() < 1e-12);
        assert!((s - DEFAULT_EQ_SEL).abs() < 1e-12);
    }

    #[test]
    fn restriction_and_join_dispatch_by_proc_oid() {
        let mut root = root();
        // oprrest = eqsel (101) -> DEFAULT_EQ_SEL.
        let r = restriction_selectivity(&mut root, Oid::new(F_EQSEL), Oid::new(96), &[], 1);
        assert!((r - DEFAULT_EQ_SEL).abs() < 1e-12);
        // oprrest = scalarltsel (103) -> DEFAULT_INEQ_SEL.
        let r = restriction_selectivity(&mut root, Oid::new(F_SCALARLTSEL), Oid::new(97), &[], 1);
        assert!((r - DEFAULT_INEQ_SEL).abs() < 1e-12);
        // oprjoin = eqjoinsel (105) -> 1/200.
        let sj = sjinfo();
        let j = join_selectivity(&mut root, Oid::new(F_EQJOINSEL), Oid::new(96), &[], JoinType::INNER, &sj);
        assert!((j - DEFAULT_EQ_SEL).abs() < 1e-12);
        let _ = InvalidOid;
    }
}
