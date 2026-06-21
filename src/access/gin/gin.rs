//! access/gin.h - Public header file for Generalized Inverted Index access method.

use std::ffi::c_int;

use crate::c::{int32, int64};
use crate::postgres::Datum;
use crate::storage::block::BlockNumber;
use crate::storage::ipc::shm_toc::shm_toc;
use crate::utils::rel::Relation;

// dsm_segment is an opaque type referenced by _gin_parallel_build_main; not yet
// defined in this tree.
// TODO: dedup - replace with canonical storage/ipc/dsm.h translation when available.
pub enum dsm_segment {}

/*
 * amproc indexes for inverted indexes.
 */
pub const GIN_COMPARE_PROC: c_int = 1;
pub const GIN_EXTRACTVALUE_PROC: c_int = 2;
pub const GIN_EXTRACTQUERY_PROC: c_int = 3;
pub const GIN_CONSISTENT_PROC: c_int = 4;
pub const GIN_COMPARE_PARTIAL_PROC: c_int = 5;
pub const GIN_TRICONSISTENT_PROC: c_int = 6;
pub const GIN_OPTIONS_PROC: c_int = 7;
pub const GINNProcs: c_int = 7;

/*
 * searchMode settings for extractQueryFn.
 */
pub const GIN_SEARCH_MODE_DEFAULT: c_int = 0;
pub const GIN_SEARCH_MODE_INCLUDE_EMPTY: c_int = 1;
pub const GIN_SEARCH_MODE_ALL: c_int = 2;
pub const GIN_SEARCH_MODE_EVERYTHING: c_int = 3; /* for internal use only */

/*
 * Constant definition for progress reporting.  Phase numbers must match
 * ginbuildphasename.
 */
/* PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE is 1 (see progress.h) */
pub const PROGRESS_GIN_PHASE_INDEXBUILD_TABLESCAN: c_int = 2;
pub const PROGRESS_GIN_PHASE_PERFORMSORT_1: c_int = 3;
pub const PROGRESS_GIN_PHASE_MERGE_1: c_int = 4;
pub const PROGRESS_GIN_PHASE_PERFORMSORT_2: c_int = 5;
pub const PROGRESS_GIN_PHASE_MERGE_2: c_int = 6;

/*
 * GinStatsData represents stats data for planner use
 */
#[repr(C)]
pub struct GinStatsData {
    pub nPendingPages: BlockNumber,
    pub nTotalPages: BlockNumber,
    pub nEntryPages: BlockNumber,
    pub nDataPages: BlockNumber,
    pub nEntries: int64,
    pub ginVersion: int32,
}

/*
 * A ternary value used by tri-consistent functions.
 *
 * This must be of the same size as a bool because some code will cast a
 * pointer to a bool to a pointer to a GinTernaryValue.
 */
pub type GinTernaryValue = std::ffi::c_char;

// StaticAssertDecl(sizeof(GinTernaryValue) == sizeof(bool), ...) - bool is one
// byte in this representation; static assert elided.

pub const GIN_FALSE: GinTernaryValue = 0; /* item is not present / does not match */
pub const GIN_TRUE: GinTernaryValue = 1; /* item is present / matches */
pub const GIN_MAYBE: GinTernaryValue = 2; /* don't know if item is present / don't
                                           * know if matches */

#[inline]
pub fn DatumGetGinTernaryValue(X: Datum) -> GinTernaryValue {
    X as GinTernaryValue
}

#[inline]
pub fn GinTernaryValueGetDatum(X: GinTernaryValue) -> Datum {
    X as Datum
}

// #define PG_RETURN_GIN_TERNARY_VALUE(x) return GinTernaryValueGetDatum(x)
#[macro_export]
macro_rules! PG_RETURN_GIN_TERNARY_VALUE {
    ($x:expr) => {
        return $crate::access::gin::gin::GinTernaryValueGetDatum($x)
    };
}

/* GUC parameters */
pub static mut GinFuzzySearchLimit: c_int = 0;
pub static mut gin_pending_list_limit: c_int = 0;

/* ginutil.c */
pub unsafe fn ginGetStats(index: Relation, stats: *mut GinStatsData) { unimplemented!() }

pub unsafe fn ginUpdateStats(
    index: Relation,
    stats: *const GinStatsData,
    is_build: bool,
) { unimplemented!() }

pub unsafe fn _gin_parallel_build_main(seg: *mut dsm_segment, toc: *mut shm_toc) { unimplemented!() }
