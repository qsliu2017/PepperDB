//! Translated from PostgreSQL src/include/access/gin.h

use crate::postgres::Datum;
use crate::storage::block::BlockNumber;
use crate::utils::rel::Relation;

// amproc indexes for inverted indexes (support proc numbers; ordinals, not flags).
pub const GIN_COMPARE_PROC: i32 = 1;
pub const GIN_EXTRACTVALUE_PROC: i32 = 2;
pub const GIN_EXTRACTQUERY_PROC: i32 = 3;
pub const GIN_CONSISTENT_PROC: i32 = 4;
pub const GIN_COMPARE_PARTIAL_PROC: i32 = 5;
pub const GIN_TRICONSISTENT_PROC: i32 = 6;
pub const GIN_OPTIONS_PROC: i32 = 7;
pub const GINNProcs: i32 = 7;

// searchMode settings for extractQueryFn.
pub const GIN_SEARCH_MODE_DEFAULT: i32 = 0;
pub const GIN_SEARCH_MODE_INCLUDE_EMPTY: i32 = 1;
pub const GIN_SEARCH_MODE_ALL: i32 = 2;
pub const GIN_SEARCH_MODE_EVERYTHING: i32 = 3; // for internal use only

// Progress reporting phase numbers (must match ginbuildphasename).
pub const PROGRESS_GIN_PHASE_INDEXBUILD_TABLESCAN: i32 = 2;
pub const PROGRESS_GIN_PHASE_PERFORMSORT_1: i32 = 3;
pub const PROGRESS_GIN_PHASE_MERGE_1: i32 = 4;
pub const PROGRESS_GIN_PHASE_PERFORMSORT_2: i32 = 5;
pub const PROGRESS_GIN_PHASE_MERGE_2: i32 = 6;

/// GinStatsData represents stats data for planner use.
pub struct GinStatsData {
    pub n_pending_pages: BlockNumber,
    pub n_total_pages: BlockNumber,
    pub n_entry_pages: BlockNumber,
    pub n_data_pages: BlockNumber,
    pub n_entries: i64,
    pub gin_version: i32,
}

/// A ternary value used by tri-consistent functions (same size as bool).
pub type GinTernaryValue = i8;

pub const GIN_FALSE: GinTernaryValue = 0; // not present / does not match
pub const GIN_TRUE: GinTernaryValue = 1; // present / matches
pub const GIN_MAYBE: GinTernaryValue = 2; // don't know

pub fn DatumGetGinTernaryValue(x: Datum) -> GinTernaryValue {
    x.0 as GinTernaryValue
}

pub fn GinTernaryValueGetDatum(x: GinTernaryValue) -> Datum {
    Datum(x as usize)
}

// PG_RETURN_GIN_TERNARY_VALUE(x) -> return GinTernaryValueGetDatum(x)

// GUC parameters
pub static mut GinFuzzySearchLimit: i32 = 0;
pub static mut gin_pending_list_limit: i32 = 0;

// ginutil.c
pub fn ginGetStats(index: &Relation) -> GinStatsData {
    let _ = index;
    unimplemented!()
}

pub fn ginUpdateStats(index: &Relation, stats: &GinStatsData, is_build: bool) {
    let _ = (index, stats, is_build);
    unimplemented!()
}

// _gin_parallel_build_main(dsm_segment *seg, shm_toc *toc): shmem/shm_toc
// parallel-worker entry collapses under the single-process model; args dropped.
pub fn _gin_parallel_build_main() {
    unimplemented!()
}
