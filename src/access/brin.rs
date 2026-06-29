//! Translated from PostgreSQL src/include/access/brin.h

use crate::storage::block::BlockNumber;
use std::sync::Arc;
use crate::utils::rel::RelationData;

/// Storage type for BRIN's reloptions. On-disk (varlena-prefixed reloptions).
#[repr(C)]
pub struct BrinOptions {
    pub vl_len_: i32, // varlena header (do not touch directly!)
    pub pages_per_range: BlockNumber,
    pub autosummarize: bool,
}

/// BrinStatsData represents stats data for planner use.
pub struct BrinStatsData {
    pub pages_per_range: BlockNumber,
    pub revmap_num_pages: BlockNumber,
}

pub const BRIN_DEFAULT_PAGES_PER_RANGE: BlockNumber = 128;

/// BrinGetPagesPerRange: rd_options ? options->pagesPerRange : default.
pub fn brin_get_pages_per_range(relation: &Arc<RelationData>) -> BlockNumber {
    let _ = relation;
    unimplemented!()
}

/// BrinGetAutoSummarize: rd_options ? options->autosummarize : false.
pub fn brin_get_auto_summarize(relation: &Arc<RelationData>) -> bool {
    let _ = relation;
    unimplemented!()
}

pub fn brinGetStats(index: &Arc<RelationData>) -> BrinStatsData {
    let _ = index;
    unimplemented!()
}

// _brin_parallel_build_main(dsm_segment *seg, shm_toc *toc): the shmem/shm_toc
// parallel-worker entry collapses under the single-process model; args dropped.
pub fn _brin_parallel_build_main() {
    unimplemented!()
}
