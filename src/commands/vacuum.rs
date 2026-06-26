//! Translated from PostgreSQL src/include/commands/vacuum.h
//! Header for the vacuum cleaner and statistics analyzer.

#![allow(
    clippy::needless_pass_by_value,
    reason = "TODO(stub): drop when implemented; hollow stubs mirror PG signatures 1:1; real impl consumes params"
)]

use bitflags::bitflags;

use crate::access::genam::{IndexBulkDeleteResult, IndexVacuumInfo};
use crate::access::htup::HeapTuple;
use crate::access::tidstore::TidStore;
use crate::access::tupdesc::TupleDesc;
use crate::c::{MultiXactId, TransactionId, bits32};
use crate::catalog::pg_class::Form_pg_class;
use crate::catalog::pg_statistic::STATISTIC_NUM_SLOTS;
use crate::catalog::pg_type::Form_pg_type;
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::VacuumStmt;
use crate::nodes::primnodes::RangeVar;
use crate::parser::parse_node::ParseState;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::storage::block::BlockNumber;
use crate::storage::buf::BufferAccessStrategy;
use crate::storage::lock::LOCKMODE;
use crate::utils::memutils::MemoryContext;
use crate::utils::relcache::Relation;

const SLOTS: usize = STATISTIC_NUM_SLOTS as usize;

bitflags! {
    /// Flags for `amparallelvacuumoptions` controlling bulkdelete/vacuumcleanup
    /// participation in parallel vacuum. Composite mask `MAX_VALID_VALUE`.
    /// Per bitflags-port.md appendix B (PARTIAL: composite mask).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct VacuumOption: u32 {
        const PARALLEL_BULKDEL = 1 << 0;
        const PARALLEL_COND_CLEANUP = 1 << 1;
        const PARALLEL_CLEANUP = 1 << 2;
    }
}

impl VacuumOption {
    /// `VACUUM_OPTION_NO_PARALLEL` - both disabled by default.
    pub const NO_PARALLEL: Self = Self::empty();
    /// `VACUUM_OPTION_MAX_VALID_VALUE` = `(1 << 3) - 1`.
    pub const MAX_VALID_VALUE: Self = Self::all();
}

bitflags! {
    /// Flag bits for `VacuumParams::options`. Per bitflags-port.md appendix A
    /// (GOOD: single-bit set).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct VacOpt: bits32 {
        const VACUUM = 0x01;
        const ANALYZE = 0x02;
        const VERBOSE = 0x04;
        const FREEZE = 0x08;
        const FULL = 0x10;
        const SKIP_LOCKED = 0x20;
        const PROCESS_MAIN = 0x40;
        const PROCESS_TOAST = 0x80;
        const DISABLE_PAGE_SKIPPING = 0x100;
        const SKIP_DATABASE_STATS = 0x200;
        const ONLY_DATABASE_STATS = 0x400;
    }
}

/// Abstract type for parallel vacuum state (opaque, in commands/vacuumparallel.c).
pub struct ParallelVacuumState {
    _private: [u8; 0],
}

/// `VacAttrStatsP` - pointer to a `VacAttrStats`.
pub type VacAttrStatsP = *mut VacAttrStats; // TODO(ptr)

/// fetchfunc: accesses a column value from a sample row; returns the Datum and
/// its null flag. C: `Datum (*AnalyzeAttrFetchFunc)(VacAttrStatsP, int rownum,
/// bool *isNull)` - the `bool *isNull` out-param folds into `Option<Datum>`.
pub type AnalyzeAttrFetchFunc = fn(stats: VacAttrStatsP, rownum: i32) -> Option<Datum>;

/// compute_stats callback. C is a fn pointer (`AnalyzeAttrComputeStatsFunc`);
/// per function-mapping a runtime callback carrying `void *extra_data` -> a
/// boxed closure, stored in `VacAttrStats::compute_stats`.
pub type AnalyzeAttrComputeStatsFunc =
    Box<dyn FnMut(VacAttrStatsP, AnalyzeAttrFetchFunc, i32, f64)>;

/// ANALYZE builds one of these per column to be analyzed. In-memory.
pub struct VacAttrStats {
    // Set up by ANALYZE before invoking the type-specific typanalyze function.
    pub attstattarget: i32,         // -1 to use default
    pub attrtypid: Oid,             // type of data being analyzed
    pub attrtypmod: i32,            // typmod of data being analyzed
    pub attrtype: Form_pg_type,     // copy of pg_type row for attrtypid
    pub attrcollid: Oid,            // collation of data being analyzed
    pub anl_context: MemoryContext, // where to save long-lived data

    // Filled in by the typanalyze routine unless it returns false.
    pub compute_stats: Option<AnalyzeAttrComputeStatsFunc>,
    pub minrows: i32,                                // minimum # of rows wanted for stats
    pub extra_data: Option<Box<dyn core::any::Any>>, // extra type-specific data

    // Filled in by the compute_stats routine.
    pub stats_valid: bool,
    pub stanullfrac: f32, // fraction of entries that are NULL
    pub stawidth: i32,    // average width of column values
    pub stadistinct: f32, // # distinct values
    pub stakind: [i16; SLOTS],
    pub staop: [Oid; SLOTS],
    pub stacoll: [Oid; SLOTS],
    pub numnumbers: [i32; SLOTS],
    pub stanumbers: [Option<Vec<f32>>; SLOTS],
    pub numvalues: [i32; SLOTS],
    pub stavalues: [Option<Vec<Datum>>; SLOTS],

    // Describe the stavalues[n] element types.
    pub statypid: [Oid; SLOTS],
    pub statyplen: [i16; SLOTS],
    pub statypbyval: [bool; SLOTS],
    pub statypalign: [u8; SLOTS],

    // Private to the main ANALYZE code.
    pub tupattnum: i32,       // attribute number within tuples
    pub rows: *mut HeapTuple, // access info for std fetch function // TODO(ptr)
    pub tup_desc: TupleDesc,
    pub exprvals: *mut Datum, // access info for index fetch function // TODO(ptr)
    pub exprnulls: *mut bool, // TODO(ptr)
    pub rowstride: i32,
}

/// Values used by `index_cleanup` and `truncate` params.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VacOptValue {
    Unspecified = 0,
    Auto,
    Disabled,
    Enabled,
}

/// Parameters customizing behavior of VACUUM and ANALYZE.
/// Resolves the `crate::commands::vacuum::VacuumParams` forward-decl that
/// tableam.h references. In-memory.
#[derive(Debug, Clone, Copy)]
pub struct VacuumParams {
    pub options: VacOpt,
    pub freeze_min_age: i32,
    pub freeze_table_age: i32,
    pub multixact_freeze_min_age: i32,
    pub multixact_freeze_table_age: i32,
    pub is_wraparound: bool,
    pub log_min_duration: i32,
    pub index_cleanup: VacOptValue,
    pub truncate: VacOptValue,
    pub toast_parent: Oid,
    pub max_eager_freeze_failure_rate: f64,
    pub nworkers: i32,
}

/// Immutable cutoffs used by a VACUUM operation. In-memory.
#[derive(Debug, Clone, Copy)]
pub struct VacuumCutoffs {
    pub relfrozenxid: TransactionId,
    pub relminmxid: MultiXactId,
    pub oldest_xmin: TransactionId,
    pub oldest_mxact: MultiXactId,
    pub freeze_limit: TransactionId,
    pub multixact_cutoff: MultiXactId,
}

/// Supplemental information for dead tuple TID storage. In-memory.
#[derive(Debug, Clone, Copy)]
pub struct VacDeadItemsInfo {
    pub max_bytes: usize, // maximum bytes TidStore can use
    pub num_items: i64,   // current # of entries
}

/// Maximum value for `default_statistics_target` and per-column targets.
pub const MAX_STATISTICS_TARGET: i32 = 10000;

// GUC parameters (process-global -> session state later; plain statics for now).
pub static mut default_statistics_target: i32 = 100;
pub static mut vacuum_freeze_min_age: i32 = 0;
pub static mut vacuum_freeze_table_age: i32 = 0;
pub static mut vacuum_multixact_freeze_min_age: i32 = 0;
pub static mut vacuum_multixact_freeze_table_age: i32 = 0;
pub static mut vacuum_failsafe_age: i32 = 0;
pub static mut vacuum_multixact_failsafe_age: i32 = 0;
pub static mut track_cost_delay_timing: bool = false;
pub static mut vacuum_truncate: bool = true;
pub static mut vacuum_max_eager_freeze_failure_rate: f64 = 0.0;

// Variables for cost-based parallel vacuum (shmem -> Arc/atomics later).
pub static mut VacuumCostBalanceLocal: i32 = 0;
pub static mut VacuumFailsafeActive: bool = false;
pub static mut vacuum_cost_delay: f64 = 0.0;
pub static mut vacuum_cost_limit: i32 = 0;
pub static mut parallel_vacuum_worker_delay_ns: i64 = 0;

// in commands/vacuum.c
pub fn ExecVacuum(pstate: &mut ParseState, vacstmt: &VacuumStmt, is_top_level: bool) {
    unimplemented!()
}

pub fn vacuum(
    relations: &[Box<Node>],
    params: &VacuumParams,
    bstrategy: Option<&BufferAccessStrategy>,
    vac_context: MemoryContext,
    is_top_level: bool,
) {
    unimplemented!()
}

/// C: `void vac_open_indexes(rel, lockmode, int *nindexes, Relation **Irel)` -
/// the two out-params fold into the returned `Vec`.
pub fn vac_open_indexes(relation: Relation, lockmode: LOCKMODE) -> Vec<Relation> {
    unimplemented!()
}

pub fn vac_close_indexes(nindexes: i32, irel: &mut [Relation], lockmode: LOCKMODE) {
    unimplemented!()
}

pub fn vac_estimate_reltuples(
    relation: Relation,
    total_pages: BlockNumber,
    scanned_pages: BlockNumber,
    scanned_tuples: f64,
) -> f64 {
    unimplemented!()
}

/// C: out-params `bool *frozenxid_updated, bool *minmulti_updated` -> tuple.
pub fn vac_update_relstats(
    relation: Relation,
    num_pages: BlockNumber,
    num_tuples: f64,
    num_all_visible_pages: BlockNumber,
    num_all_frozen_pages: BlockNumber,
    hasindex: bool,
    frozenxid: TransactionId,
    minmulti: MultiXactId,
    in_outer_xact: bool,
) -> (bool, bool) {
    unimplemented!()
}

/// C: `bool vacuum_get_cutoffs(rel, params, struct VacuumCutoffs *cutoffs)` -
/// the success-bool + out-param fold into `Option`.
pub fn vacuum_get_cutoffs(rel: Relation, params: &VacuumParams) -> Option<VacuumCutoffs> {
    unimplemented!()
}

pub fn vacuum_xid_failsafe_check(cutoffs: &VacuumCutoffs) -> bool {
    unimplemented!()
}

pub fn vac_update_datfrozenxid() {
    unimplemented!()
}

pub fn vacuum_delay_point(is_analyze: bool) {
    unimplemented!()
}

pub fn vacuum_is_permitted_for_relation(relid: Oid, reltuple: Form_pg_class, options: VacOpt) -> bool {
    unimplemented!()
}

pub fn vacuum_open_relation(
    relid: Oid,
    relation: Option<&RangeVar>,
    options: VacOpt,
    verbose: bool,
    lmode: LOCKMODE,
) -> Relation {
    unimplemented!()
}

pub fn vac_bulkdel_one_index(
    ivinfo: &mut IndexVacuumInfo,
    istat: Option<Box<IndexBulkDeleteResult>>,
    dead_items: &TidStore,
    dead_items_info: &VacDeadItemsInfo,
) -> Box<IndexBulkDeleteResult> {
    unimplemented!()
}

pub fn vac_cleanup_one_index(
    ivinfo: &mut IndexVacuumInfo,
    istat: Option<Box<IndexBulkDeleteResult>>,
) -> Box<IndexBulkDeleteResult> {
    unimplemented!()
}

// In postmaster/autovacuum.c
pub fn AutoVacuumUpdateCostLimit() {
    unimplemented!()
}

pub fn VacuumUpdateCosts() {
    unimplemented!()
}

// in commands/vacuumparallel.c
pub fn parallel_vacuum_init(
    rel: Relation,
    indrels: &mut [Relation],
    nindexes: i32,
    nrequested_workers: i32,
    vac_work_mem: i32,
    elevel: i32,
    bstrategy: Option<&BufferAccessStrategy>,
) -> Box<ParallelVacuumState> {
    unimplemented!()
}

pub fn parallel_vacuum_end(pvs: &mut ParallelVacuumState, istats: &mut [Box<IndexBulkDeleteResult>]) {
    unimplemented!()
}

/// C: out-param `VacDeadItemsInfo **dead_items_info_p` -> tuple.
pub fn parallel_vacuum_get_dead_items(pvs: &mut ParallelVacuumState) -> (TidStore, VacDeadItemsInfo) {
    unimplemented!()
}

pub fn parallel_vacuum_reset_dead_items(pvs: &mut ParallelVacuumState) {
    unimplemented!()
}

pub fn parallel_vacuum_bulkdel_all_indexes(
    pvs: &mut ParallelVacuumState,
    num_table_tuples: i64,
    num_index_scans: i32,
) {
    unimplemented!()
}

pub fn parallel_vacuum_cleanup_all_indexes(
    pvs: &mut ParallelVacuumState,
    num_table_tuples: i64,
    num_index_scans: i32,
    estimated_count: bool,
) {
    unimplemented!()
}

// parallel_vacuum_main(dsm_segment *seg, shm_toc *toc): the dsm/shm-toc args are
// shmem worker plumbing (LEVEL2-NOTES: shmem -> Arc/tokio); dropped here.
pub fn parallel_vacuum_main() {
    unimplemented!()
}

// in commands/analyze.c
pub fn analyze_rel(
    relid: Oid,
    relation: Option<&RangeVar>,
    params: &VacuumParams,
    va_cols: &[Box<Node>],
    in_outer_xact: bool,
    bstrategy: Option<&BufferAccessStrategy>,
) {
    unimplemented!()
}

pub fn std_typanalyze(stats: &mut VacAttrStats) -> bool {
    unimplemented!()
}

// in utils/misc/sampling.c (duplicate of declarations in utils/sampling.h)
pub fn anl_random_fract() -> f64 {
    unimplemented!()
}

pub fn anl_init_selection_state(n: i32) -> f64 {
    unimplemented!()
}

/// C: `double anl_get_next_S(double t, int n, double *stateptr)` - the in/out
/// `*stateptr` becomes `&mut f64`.
pub fn anl_get_next_S(t: f64, n: i32, stateptr: &mut f64) -> f64 {
    unimplemented!()
}
