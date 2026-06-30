//! Selectivity functions for standard operators. Translated from
//! backend/utils/adt/selfuncs.c (disposition: grow).
//!
//! These are the per-operator restriction (`oprrest`) and join (`oprjoin`)
//! selectivity estimators. PG reads `pg_statistic` (the ANALYZE stats) to compute
//! a real estimate; until ANALYZE lands (M13) those stats do not exist, so every
//! estimator here takes the NO-STATISTICS DEFAULT path:
//!   - `eqsel`        -> `1 / get_variable_numdistinct` = 1/200 = `DEFAULT_EQ_SEL`.
//!   - `neqsel`       -> `1 - eqsel`.
//!   - `scalar{lt,gt,le,ge}sel` -> `DEFAULT_INEQ_SEL`.
//!   - `eqjoinsel`    -> `1 / max(nd1, nd2)` = 1/200 (the default join selectivity
//!     that drives the join-rel row estimate in costsize.rs).
//!   - `scalar*joinsel` -> `DEFAULT_INEQ_SEL`.
//!
//! With no stats `examine_variable` returns no `statsTuple` and
//! `get_variable_numdistinct` returns `DEFAULT_NUM_DISTINCT`; that is the only path
//! reachable here. The real histogram/MCV machinery grows with ANALYZE.
//!
//! The estimators are bound to operators through the seeded `pg_operator.oprrest` /
//! `oprjoin` columns: `clause_selectivity` looks up an operator's `oprrest` proc OID
//! (`get_oprrest`) and dispatches to `restriction_selectivity`; the join path uses
//! `get_oprjoin` + `join_selectivity`.

use crate::nodes::nodes::{JoinType, Node, Selectivity};
use crate::nodes::pathnodes::{PlannerInfo, SpecialJoinInfo};
use crate::postgres_ext::Oid;

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
/// ("var = const"). No-stats path: `1 / get_variable_numdistinct` = 1/200 =
/// `DEFAULT_EQ_SEL`. `_root`/`_operator`/`_args`/`_var_relid` are the PG
/// `(root, operator, args, varRelid)` parameters; they select the variable and its
/// stats, which are absent here, so every variable yields the default.
pub fn eqsel(
    _root: &mut PlannerInfo,
    _operator: Oid,
    _args: &[Node],
    _var_relid: i32,
) -> Selectivity {
    clamp_probability(1.0 / get_variable_numdistinct())
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

/// PG `scalarltsel`/`scalarlesel`/`scalargtsel`/`scalargesel` (no-stats path): an
/// ordering-comparison restriction has no histogram, so `scalarineqsel` returns
/// `DEFAULT_INEQ_SEL`.
pub fn scalarineqsel_default(
    _root: &mut PlannerInfo,
    _operator: Oid,
    _args: &[Node],
    _var_relid: i32,
) -> Selectivity {
    DEFAULT_INEQ_SEL
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
        F_SCALARLTSEL | F_SCALARGTSEL | F_SCALARLESEL | F_SCALARGESEL => {
            scalarineqsel_default(root, operator, args, var_relid)
        }
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
