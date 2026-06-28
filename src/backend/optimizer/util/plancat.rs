//! Routines for accessing the system catalogs for the planner. Translated from
//! backend/optimizer/util/plancat.c.
//!
//! Non-type-centric free functions; bodies here as snake_case `pub fn`s,
//! re-exported from `crate::optimizer::plancat` under the C names.
//!
//! Disposition: `grow`. M2's live path is `get_relation_info` for a plain heap
//! relation: open the (already-locked, relcache-resident) relation, fill the
//! `RelOptInfo`'s attribute range / tablespace / size estimates, and leave the
//! index list empty (no index paths until M6). The open is a synchronous relcache
//! lookup (the relation was locked during parse analysis); `estimate_rel_size`
//! reads the cached `relpages`/`reltuples` from pg_class for M2 rather than the
//! live smgr block count (which would color the planner async) -- the live
//! block-count path grows when stale-stats handling matters. The selectivity /
//! FDW / trigger / generated-column helpers remain grow guards (rules.md s4).

use crate::access::attnum::AttrNumber;
use crate::nodes::pathnodes::{PlannerInfo, RelOptInfo};
use crate::postgres_ext::Oid;
use crate::storage::block::BlockNumber;
use crate::utils::rel::RelationData;

/// PG `FirstLowInvalidHeapAttributeNumber` (sysattr.h): the lowest valid system
/// attribute number is -7, so `min_attr = FirstLowInvalidHeapAttributeNumber + 1`.
const FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER: AttrNumber = -7;

/// PG `get_relation_info`: gather the planner's per-relation info into `rel` from
/// the relcache entry for `relation_object_id`. M2 covers a plain heap relation:
/// attribute range, tablespace, and size estimates. The relation was already
/// locked during parse analysis, so the open is a sync relcache lookup (NoLock).
pub fn get_relation_info(
    root: &mut PlannerInfo,
    relation_object_id: Oid,
    inhparent: bool,
    rel: &mut RelOptInfo,
) {
    if inhparent {
        not_yet_reachable("get_relation_info: inheritance parent");
    }

    // table_open(relationObjectId, NoLock): the lock is already held, so this is a
    // pure relcache lookup. The entry was built during parse analysis.
    let relation = crate::backend::utils::cache::relcache::relation_id_get_relation(relation_object_id)
        .unwrap_or_else(|| not_yet_reachable("get_relation_info: relation not in relcache"));

    // SAFETY: live relcache entry held for the duration of this function.
    let rel_data: &RelationData = unsafe { &*relation };
    let relkind = unsafe { (*rel_data.rd_rel).relkind };
    let reltablespace = unsafe { (*rel_data.rd_rel).reltablespace };

    if !is_scannable_relkind(relkind) {
        not_yet_reachable("get_relation_info: non-heap relation kind");
    }

    rel.min_attr = FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER + 1;
    rel.max_attr = rel_data.number_of_attributes();
    rel.reltablespace = reltablespace;
    crate::assert!(rel.max_attr >= rel.min_attr);

    // attr_needed / attr_widths span min_attr..=max_attr; zero-initialized.
    let span = (rel.max_attr - rel.min_attr + 1) as usize;
    rel.attr_needed = vec![None; span];
    rel.attr_widths = vec![0; span];

    // Estimate relation size (pages/tuples/allvisfrac) from cached pg_class stats.
    let est = estimate_rel_size(rel_data, &rel.attr_widths);
    rel.pages = est.pages;
    rel.tuples = est.tuples;
    rel.allvisfrac = est.allvisfrac;
    rel.attr_widths = est.attr_widths;

    // Index list: minimal for M2 (no index paths until M6).
    rel.indexlist = Vec::new();
    rel.statlist = Vec::new();
    rel.rel_parallel_workers = -1;
    rel.amflags = crate::nodes::pathnodes::AmFlags::empty();

    crate::backend::utils::cache::relcache::relation_close(relation);
    let _ = root;
}

/// Out-params of `estimate_rel_size` folded into a struct.
pub struct RelSizeEstimate {
    pub attr_widths: Vec<i32>,
    pub pages: BlockNumber,
    pub tuples: f64,
    pub allvisfrac: f64,
}

/// PG `estimate_rel_size` (M2 subset): page/tuple/all-visible estimates. M2 reads
/// the cached `relpages`/`reltuples`/`relallvisible` from pg_class (set by
/// CREATE/ANALYZE). PG additionally combines `relpages` with the live block count
/// to scale `reltuples`; that live-count adjustment grows when stale stats matter.
pub fn estimate_rel_size(rel: &RelationData, cur_attr_widths: &[i32]) -> RelSizeEstimate {
    let form = rel.rd_rel;
    // SAFETY: live pg_class form on the relcache entry.
    let relpages = unsafe { (*form).relpages };
    let reltuples = unsafe { (*form).reltuples };
    let relallvisible = unsafe { (*form).relallvisible };

    let pages = if relpages <= 0 { 0 } else { relpages as BlockNumber };
    let tuples = if reltuples < 0.0 { 0.0 } else { f64::from(reltuples) };
    let allvisfrac = if pages == 0 {
        0.0
    } else {
        (f64::from(relallvisible) / f64::from(pages)).clamp(0.0, 1.0)
    };

    RelSizeEstimate {
        attr_widths: cur_attr_widths.to_vec(),
        pages,
        tuples,
        allvisfrac,
    }
}

/// Is this relkind a plain scannable table for M2?
fn is_scannable_relkind(relkind: i8) -> bool {
    relkind == crate::catalog::pg_class::RELKIND_RELATION
}

#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}
