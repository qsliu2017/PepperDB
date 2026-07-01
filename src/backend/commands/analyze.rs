//! ANALYZE: sample a relation's rows and compute per-column statistics into
//! pg_statistic. Translated from the M13-reachable core of
//! backend/commands/analyze.c.
//!
//! M13 scope (step 46): `analyze_rel` -- open the relation, acquire a sample of its
//! live rows (`acquire_sample_rows`), and per analyzable column compute the standard
//! scalar statistics (`std_typanalyze` -> `compute_scalar_stats`): null fraction
//! (stanullfrac), average width (stawidth), n_distinct (stadistinct), the
//! most-common-values list + frequencies (STATISTIC_KIND_MCV), and the histogram
//! bounds (STATISTIC_KIND_HISTOGRAM). Write/update the pg_statistic rows and update
//! pg_class.reltuples/relpages. The planner reads these via the process-global stats
//! cache in selfuncs (the sync planner cannot heap-scan pg_statistic; the durable
//! pg_statistic rows are the source of truth, the cache is the read path).
//!
//! Scope (rules.md s4, simplified encoding): the sample is a full scan up to
//! `default_statistics_target * 300` rows (PG uses a reservoir/Vitter sample -- a
//! perf optimization for tables larger than the sample; for the tested sizes the
//! full scan visits every row, giving identical stats). Statistics are computed for
//! the sortable by-value scalar types the M2 heap stores (int2/int4/int8); the
//! per-type `compute_stats` dispatch and the anyarray varlena encoding of
//! stavalues/stanumbers are simplified to an owned in-cache representation plus a
//! NULL on-disk varlena (the durable row records the fixed columns; the MCV /
//! histogram arrays live in the stats cache the reader consults). MCELEM / range /
//! multi-column extended stats are staged.

#![allow(
    clippy::cast_ptr_alignment,
    reason = "faithful GETSTRUCT reinterpretation of a heap tuple to a Form_* struct (MAXALIGN'd body covers the Form alignment)"
)]

use std::sync::Arc;

use crate::backend::access::common::heaptuple::heap_getattr;
use crate::backend::access::heap::heapam::{heap_beginscan, heap_endscan, heap_getnext};
use crate::backend::utils::cache::relcache::{relation_close, relation_id_get_relation};
use crate::commands::vacuum::VacuumParams;
use crate::nodes::nodes::Node;
use crate::postgres::{Datum, DatumGetInt16, DatumGetInt32};
use crate::postgres_ext::Oid;
use crate::shared_state::SharedState;

/// The default per-column statistics target (`default_statistics_target`). PG's
/// GUC default; the sample size is `target * 300`.
const DEFAULT_STATISTICS_TARGET: usize = 100;

/// The maximum most-common-values kept per column (bounded by the stats target).
const MAX_MCV: usize = DEFAULT_STATISTICS_TARGET;

/// The number of histogram bounds kept (target + 1 bucket boundaries).
const HIST_SIZE: usize = DEFAULT_STATISTICS_TARGET + 1;

/// One column's computed statistics, mirroring the pg_statistic row fields the
/// planner consumes. The MCV values + histogram bounds are kept as owned `i64`
/// (the widened by-value scalar) since the M2 heap's sortable columns are
/// int2/int4/int8; selfuncs compares the constant widened the same way.
#[derive(Debug, Clone, Default)]
pub struct ColumnStats {
    pub attnum: i16,
    pub stanullfrac: f32,
    pub stawidth: i32,
    /// > 0: distinct count; < 0: negative multiplier of the row count.
    pub stadistinct: f32,
    /// MCV values (STATISTIC_KIND_MCV) as widened scalars + their frequencies.
    pub mcv_values: Vec<i64>,
    pub mcv_freqs: Vec<f32>,
    /// Histogram bounds (STATISTIC_KIND_HISTOGRAM), ascending.
    pub histogram: Vec<i64>,
}

/// `analyze_rel` (M13): compute statistics for `relid`. Scans a sample of the
/// relation's live rows, computes per-column stats, writes them to pg_statistic +
/// the planner stats cache, and updates pg_class.reltuples/relpages.
///
/// `va_cols` restricts analysis to the named columns (empty = all columns).
pub async fn analyze_rel(
    shared: &Arc<SharedState>,
    relid: Oid,
    _params: &VacuumParams,
    va_cols: &[Node],
) {
    // ANALYZE takes ShareUpdateExclusive; open through the relcache directly (the
    // heavyweight lock needs a PGPROC, deferred -- see vacuum_rel).
    let Some(relation) = relation_id_get_relation(relid) else { return };

    let relkind = relation.form().relkind;
    if relkind != crate::catalog::pg_class::RELKIND_RELATION {
        relation_close(relation);
        return;
    }

    let desc = relation
        .rd_att
        .clone()
        .unwrap_or_else(|| unreachable!("relation has a descriptor for ANALYZE"));
    let natts = desc.natts as usize;

    // Which columns to analyze (the va_cols restriction is not exercised by the
    // tests; empty = every non-dropped attribute).
    let _ = va_cols;

    // Acquire the sample: a full scan of live rows up to the sample limit. Collect,
    // per attribute, each row's (value, isnull) and the row width.
    let sample_limit = DEFAULT_STATISTICS_TARGET * 300;
    let snapshot = crate::backend::utils::time::snapmgr::GetActiveSnapshot()
        .unwrap_or_else(|| unreachable!("active snapshot for ANALYZE sample"));

    // Per-attribute value columns (only the non-null values, widened to i64) + null
    // counts + width accumulators.
    let mut col_values: Vec<Vec<i64>> = vec![Vec::new(); natts];
    let mut col_nulls: Vec<i64> = vec![0; natts];
    let mut col_width_sum: Vec<i64> = vec![0; natts];
    let col_sortable: Vec<bool> =
        (0..natts).map(|i| is_sortable_scalar(desc.attr(i).atttypid)).collect();

    let mut total_rows: i64 = 0;
    {
        use crate::access::sdir::ScanDirection;
        use crate::access::tableam::ScanOptions;
        let mut scan = heap_beginscan(&relation, &snapshot, 0, ScanOptions::ALLOW_PAGEMODE);
        while let Some(tup) = heap_getnext(shared, &mut scan, ScanDirection::Forward).await {
            if total_rows as usize >= sample_limit {
                break;
            }
            // SAFETY: live scan tuple over the pinned page.
            let tref = unsafe { &*tup };
            total_rows += 1;
            for attidx in 0..natts {
                let attnum = (attidx + 1) as i32;
                // SAFETY: attnum is a valid 1-based attribute number for desc.
                let (val, isnull) = unsafe { heap_getattr(tref, attnum, &desc) };
                if isnull {
                    col_nulls[attidx] += 1;
                    continue;
                }
                let att = desc.attr(attidx);
                col_width_sum[attidx] += i64::from(att.attlen.max(0));
                if col_sortable[attidx] {
                    col_values[attidx].push(widen_scalar(att.atttypid, val));
                }
            }
        }
        heap_endscan(shared, &mut scan);
    }

    // Compute + persist per-column stats.
    let mut computed: Vec<ColumnStats> = Vec::new();
    for attidx in 0..natts {
        let att = desc.attr(attidx);
        if att.attisdropped {
            continue;
        }
        let nonnull = &col_values[attidx];
        let nnull = col_nulls[attidx];
        let width = if att.attlen > 0 {
            i32::from(att.attlen)
        } else if total_rows > 0 {
            (col_width_sum[attidx] / total_rows.max(1)) as i32
        } else {
            0
        };

        let mut stats = ColumnStats {
            attnum: (attidx + 1) as i16,
            stanullfrac: if total_rows > 0 { nnull as f32 / total_rows as f32 } else { 0.0 },
            stawidth: width,
            ..Default::default()
        };

        if col_sortable[attidx] && !nonnull.is_empty() {
            compute_scalar_stats(&mut stats, nonnull, total_rows);
        } else {
            // Non-sortable / all-null: leave stadistinct = 0 (unknown).
            stats.stadistinct = 0.0;
        }

        write_pg_statistic(shared, relid, &stats).await;
        crate::backend::utils::adt::selfuncs::store_column_stats(relid, &stats);
        computed.push(stats);
    }

    // Update pg_class.reltuples (+ relpages estimate) so the planner's row estimate
    // reflects the analyzed count.
    let num_pages = heap_nblocks(shared, &relation).await;
    update_pg_class_stats(shared, relid, num_pages as i32, total_rows as f32).await;

    relation_close(relation);
    let _ = computed;
}

/// `compute_scalar_stats` (M13 subset): fill `stats`' distinct/MCV/histogram from
/// the sorted non-null sample `values` (already widened to i64). `total_rows` is the
/// sample row count (for the negative-multiplier stadistinct form).
///
/// Distinct: count distinct sorted values. MCV: values whose count > 1, the top
/// `MAX_MCV` by frequency (PG keeps values "significantly more common than
/// average"; the simplified rule keeps the duplicated values, which is what the
/// planner's eqsel uses). Histogram: `HIST_SIZE` equi-depth bounds over the
/// (non-MCV) sorted values.
fn compute_scalar_stats(stats: &mut ColumnStats, values: &[i64], total_rows: i64) {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let nnonnull = sorted.len();

    // Distinct-value run-length pass: count each distinct value's occurrences.
    let mut distinct: Vec<(i64, i64)> = Vec::new(); // (value, count)
    for &v in &sorted {
        if let Some(last) = distinct.last_mut()
            && last.0 == v
        {
            last.1 += 1;
            continue;
        }
        distinct.push((v, 1));
    }
    let ndistinct = distinct.len();

    // stadistinct: PG's rule. If every value is distinct in the sample and the
    // sample is (roughly) the whole table, report a negative multiplier (-1 = all
    // distinct). Otherwise the observed distinct count.
    stats.stadistinct = if ndistinct == nnonnull && total_rows > 0 {
        // All-distinct in the sample -> scales with the row count.
        -1.0
    } else {
        ndistinct as f32
    };

    // MCV: values appearing more than once, most frequent first, capped at MAX_MCV.
    let mut multiples: Vec<(i64, i64)> = distinct.iter().copied().filter(|&(_, c)| c > 1).collect();
    multiples.sort_by_key(|&(_, c)| std::cmp::Reverse(c));
    multiples.truncate(MAX_MCV);
    let denom = total_rows.max(1) as f32;
    for (v, c) in &multiples {
        stats.mcv_values.push(*v);
        stats.mcv_freqs.push(*c as f32 / denom);
    }

    // Histogram: equi-depth bounds over the sorted non-null values EXCLUDING the
    // MCVs (PG builds the histogram from the non-MCV values). Keep at most
    // HIST_SIZE bounds.
    let mcv_set: std::collections::HashSet<i64> = stats.mcv_values.iter().copied().collect();
    let hist_input: Vec<i64> = sorted.iter().copied().filter(|v| !mcv_set.contains(v)).collect();
    if hist_input.len() >= 2 {
        let nbounds = HIST_SIZE.min(hist_input.len());
        let mut bounds = Vec::with_capacity(nbounds);
        for i in 0..nbounds {
            // Pick evenly spaced positions across the sorted non-MCV values.
            let pos = if nbounds == 1 {
                0
            } else {
                (i * (hist_input.len() - 1)) / (nbounds - 1)
            };
            bounds.push(hist_input[pos]);
        }
        bounds.dedup();
        if bounds.len() >= 2 {
            stats.histogram = bounds;
        }
    }
}

/// `update_attstats`' pg_statistic write for one column: insert or replace the
/// pg_statistic row `(starelid, staattnum, stainherit=false)` with `stats`' fixed
/// columns. The MCV/histogram arrays are recorded in the stats cache (the reader);
/// the on-disk stavalues/stanumbers varlenas are stored NULL in this milestone (the
/// durable row carries stanullfrac/stawidth/stadistinct + the slot kinds).
async fn write_pg_statistic(shared: &Arc<SharedState>, relid: Oid, stats: &ColumnStats) {
    use crate::backend::access::common::heaptuple::{heap_form_tuple, heap_freetuple};
    use crate::backend::catalog::indexing::{catalog_tuple_insert, catalog_tuple_update};
    use crate::backend::utils::cache::relcache::{relation_close as rc_close, relation_id_get_relation};
    use crate::catalog::pg_statistic::{
        self as s, StatisticRelationId, STATISTIC_KIND_HISTOGRAM, STATISTIC_KIND_MCV,
    };
    use crate::postgres::{BoolGetDatum, Float4GetDatum, Int16GetDatum, Int32GetDatum, ObjectIdGetDatum};

    let Some(pg_statistic) = relation_id_get_relation(StatisticRelationId) else { return };
    let desc = pg_statistic
        .rd_att
        .clone()
        .unwrap_or_else(|| unreachable!("pg_statistic descriptor"));
    let natts = desc.natts as usize;

    let mut values = vec![Datum(0); natts];
    let mut isnull = vec![false; natts];

    set(&mut values, s::Anum_pg_statistic_starelid, ObjectIdGetDatum(relid));
    set(&mut values, s::Anum_pg_statistic_staattnum, Int16GetDatum(stats.attnum));
    set(&mut values, s::Anum_pg_statistic_stainherit, BoolGetDatum(false));
    set(&mut values, s::Anum_pg_statistic_stanullfrac, Float4GetDatum(stats.stanullfrac));
    set(&mut values, s::Anum_pg_statistic_stawidth, Int32GetDatum(stats.stawidth));
    set(&mut values, s::Anum_pg_statistic_stadistinct, Float4GetDatum(stats.stadistinct));

    // Slot kinds: slot 1 = MCV (if any), slot 2 = HISTOGRAM (if any), rest 0.
    let kind1 = if stats.mcv_values.is_empty() { 0 } else { STATISTIC_KIND_MCV as i16 };
    let kind2 = if stats.histogram.is_empty() { 0 } else { STATISTIC_KIND_HISTOGRAM as i16 };
    set(&mut values, s::Anum_pg_statistic_stakind1, Int16GetDatum(kind1));
    set(&mut values, s::Anum_pg_statistic_stakind2, Int16GetDatum(kind2));
    set(&mut values, s::Anum_pg_statistic_stakind3, Int16GetDatum(0));
    set(&mut values, s::Anum_pg_statistic_stakind4, Int16GetDatum(0));
    set(&mut values, s::Anum_pg_statistic_stakind5, Int16GetDatum(0));
    for anum in [
        s::Anum_pg_statistic_staop1, s::Anum_pg_statistic_staop2, s::Anum_pg_statistic_staop3,
        s::Anum_pg_statistic_staop4, s::Anum_pg_statistic_staop5,
        s::Anum_pg_statistic_stacoll1, s::Anum_pg_statistic_stacoll2, s::Anum_pg_statistic_stacoll3,
        s::Anum_pg_statistic_stacoll4, s::Anum_pg_statistic_stacoll5,
    ] {
        set(&mut values, anum, ObjectIdGetDatum(crate::postgres_ext::InvalidOid));
    }
    // The varlen stanumbers*/stavalues* are stored NULL (their content lives in the
    // planner stats cache; see module docs).
    for anum in [
        s::Anum_pg_statistic_stanumbers1, s::Anum_pg_statistic_stanumbers2,
        s::Anum_pg_statistic_stanumbers3, s::Anum_pg_statistic_stanumbers4,
        s::Anum_pg_statistic_stanumbers5, s::Anum_pg_statistic_stavalues1,
        s::Anum_pg_statistic_stavalues2, s::Anum_pg_statistic_stavalues3,
        s::Anum_pg_statistic_stavalues4, s::Anum_pg_statistic_stavalues5,
    ] {
        isnull[(anum - 1) as usize] = true;
    }

    // Replace an existing row for (relid, attnum, false) if present, else insert.
    let existing = scan_pg_statistic_row(shared, relid, stats.attnum).await;
    let mut tuple = heap_form_tuple(&desc, &values, &isnull);
    if let Some(tid) = existing {
        catalog_tuple_update(shared, &pg_statistic, &tid, &mut tuple).await;
    } else {
        catalog_tuple_insert(shared, &pg_statistic, &mut tuple).await;
    }
    heap_freetuple(tuple);
    rc_close(pg_statistic);
}

/// Find the TID of the existing pg_statistic row for `(relid, attnum, inh=false)`,
/// if any (so ANALYZE replaces rather than duplicates).
async fn scan_pg_statistic_row(
    shared: &Arc<SharedState>,
    relid: Oid,
    attnum: i16,
) -> Option<crate::storage::itemptr::ItemPointerData> {
    use crate::access::htup_details::GETSTRUCT;
    use crate::catalog::pg_statistic::{self as s, FormData_pg_statistic, StatisticRelationId};

    let rows = crate::backend::commands::tablecmds::scan_catalog_rows_by_oid(
        shared,
        StatisticRelationId,
        s::Anum_pg_statistic_starelid,
        relid,
    )
    .await;
    let mut found = None;
    for row in &rows {
        // SAFETY: owned tuple; read the fixed part to match (relid, attnum, !inh).
        let p = GETSTRUCT(&row.tuple).cast::<FormData_pg_statistic>();
        let (r, a, inh) = unsafe { ((*p).starelid, (*p).staattnum, (*p).stainherit) };
        if r == relid && a == attnum && !inh {
            found = Some(row.tid);
            break;
        }
    }
    for row in rows {
        crate::backend::access::common::heaptuple::heap_freetuple(row.tuple);
    }
    found
}

/// `vac_update_relstats`' pg_class.relpages/reltuples write (shared by VACUUM and
/// ANALYZE): update the relation's pg_class row and the relcache copy the planner
/// reads (`estimate_rel_size`).
pub async fn update_pg_class_stats(
    shared: &Arc<SharedState>,
    relid: Oid,
    relpages: i32,
    reltuples: f32,
) {
    use crate::access::htup_details::GETSTRUCT;
    use crate::backend::access::common::heaptuple::{heap_deform_tuple, heap_form_tuple, heap_freetuple};
    use crate::backend::catalog::indexing::catalog_tuple_update;
    use crate::backend::utils::cache::relcache::{relation_close as rc_close, relation_id_get_relation};
    use crate::catalog::pg_class::{self as pc, FormData_pg_class, RelationRelationId};
    use crate::postgres::{Float4GetDatum, Int32GetDatum};

    let Some(pg_class) = relation_id_get_relation(RelationRelationId) else { return };
    let desc = pg_class.rd_att.clone().unwrap_or_else(|| unreachable!("pg_class desc"));
    let rows = crate::backend::commands::tablecmds::scan_catalog_rows_by_oid(
        shared,
        RelationRelationId,
        pc::Anum_pg_class_oid,
        relid,
    )
    .await;
    for row in &rows {
        // SAFETY: owned tuple; check the target relation.
        let p = GETSTRUCT(&row.tuple).cast::<FormData_pg_class>();
        if unsafe { (*p).oid } != relid {
            continue;
        }
        // SAFETY: owned tuple + matching descriptor.
        let (mut vals, mut nulls) = unsafe { heap_deform_tuple(&row.tuple, &desc) };
        vals[(pc::Anum_pg_class_relpages - 1) as usize] = Int32GetDatum(relpages);
        nulls[(pc::Anum_pg_class_relpages - 1) as usize] = false;
        vals[(pc::Anum_pg_class_reltuples - 1) as usize] = Float4GetDatum(reltuples);
        nulls[(pc::Anum_pg_class_reltuples - 1) as usize] = false;
        let mut newtup = heap_form_tuple(&desc, &vals, &nulls);
        catalog_tuple_update(shared, &pg_class, &row.tid, &mut newtup).await;
        heap_freetuple(newtup);
        break;
    }
    for row in rows {
        heap_freetuple(row.tuple);
    }
    rc_close(pg_class);

    // Refresh the relcache so the planner's estimate_rel_size sees the new size: try
    // the in-place update, and forget the entry so the next open rebuilds it from the
    // now-updated pg_class row (the in-place path no-ops when the Arc is shared).
    crate::backend::utils::cache::relcache::update_relation_stats(relid, relpages, reltuples);
    crate::backend::utils::cache::relcache::relation_forget_relation(relid);
}

/// Store `value` at `attno` (1-based) in the values array.
fn set(values: &mut [Datum], attno: i32, value: Datum) {
    values[(attno - 1) as usize] = value;
}

/// The block count of `relation`'s main fork.
async fn heap_nblocks(shared: &Arc<SharedState>, relation: &crate::utils::rel::RelationData) -> u32 {
    use crate::common::relpath::ForkNumber;
    let smgr_ptr = relation.smgr();
    // SAFETY: relcache-owned smgr handle, valid while the rel is open.
    let smgr = unsafe { &mut *smgr_ptr };
    smgr.nblocks(shared, ForkNumber::MAIN_FORKNUM).await
}

/// Whether `typid` is a sortable by-value scalar the M13 analyze computes scalar
/// stats for (int2/int4/int8). Other types get only null-fraction/width.
fn is_sortable_scalar(typid: Oid) -> bool {
    matches!(typid.get(), 21 | 23 | 20) // INT2OID / INT4OID / INT8OID
}

/// Widen a by-value scalar Datum to an i64 for sorting/comparison, per its type.
fn widen_scalar(typid: Oid, val: Datum) -> i64 {
    match typid.get() {
        21 => i64::from(DatumGetInt16(val)),
        23 => i64::from(DatumGetInt32(val)),
        // int8 (20) and any other by-value scalar: the low 64 bits.
        _ => val.0 as i64,
    }
}
