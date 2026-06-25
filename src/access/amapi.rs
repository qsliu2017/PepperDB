//! Translated from PostgreSQL src/include/access/amapi.h
//! API for Postgres index access methods.
//!
//! Per routine-struct.md: the `IndexAmRoutine` vtable becomes a base trait
//! (`IndexAm`) plus an `AmCaps` bitflags set for the `amcan*` bools, capability
//! supertraits for the optional callback groups, and a scan handle trait
//! (`IndexScan`) whose `amendscan` is `Drop`. The six built-ins are a closed
//! `IndexAmKind` enum (static dispatch); extension AMs are the open fn-pointer
//! case. `amparallelvacuumoptions` is its own bitflags word.

use bitflags::bitflags;

use crate::access::cmptype::CompareType;
use crate::access::genam::{
    IndexBuildResult, IndexBulkDeleteCallback, IndexBulkDeleteResult, IndexUniqueCheck,
    IndexVacuumInfo,
};
use crate::access::relscan::ParallelIndexScanDescData;
use crate::access::sdir::ScanDirection;
use crate::access::skey::ScanKeyData;
use crate::access::stratnum::StrategyNumber;
use crate::c::bytea;
use crate::nodes::execnodes::IndexInfo;
use crate::nodes::pathnodes::{IndexPath, PlannerInfo};
use crate::nodes::tidbitmap::TIDBitmap;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::storage::itemptr::ItemPointerData;
use crate::utils::relcache::Relation;

/// Properties for the amproperty API. Core-known properties; an AM can also
/// define its own by matching the string property name.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexAMProperty {
    UNKNOWN = 0, // anything not known to core code
    ASC,         // column properties
    DESC,
    NULLS_FIRST,
    NULLS_LAST,
    ORDERABLE,
    DISTANCE_ORDERABLE,
    RETURNABLE,
    SEARCH_ARRAY,
    SEARCH_NULLS,
    CLUSTERABLE, // index properties
    INDEX_SCAN,
    BITMAP_SCAN,
    BACKWARD_SCAN,
    CAN_ORDER, // AM properties
    CAN_UNIQUE,
    CAN_MULTI_COL,
    CAN_EXCLUDE,
    CAN_INCLUDE,
}

/// Tracks operators and support functions while building/adding to an opclass
/// or opfamily. amadjustmembers functions may alter the "ref" fields.
pub struct OpFamilyMember {
    pub is_func: bool,       // is this an operator, or support func?
    pub object: Oid,         // operator or support func's OID
    pub number: i32,         // strategy or support func number
    pub lefttype: Oid,       // lefttype
    pub righttype: Oid,      // righttype
    pub sortfamily: Oid,     // ordering operator's sort opfamily, or 0
    pub ref_is_hard: bool,   // hard or soft dependency?
    pub ref_is_family: bool, // is dependency on opclass or opfamily?
    pub refobjid: Oid,       // OID of opclass or opfamily
}

bitflags! {
    /// The 19 `amcan*`-style capability bools. Queried at runtime by the planner
    /// via `IndexAm::capabilities()`; `am.capabilities().contains(AmCaps::BACKWARD)`
    /// reads where C reads `amroutine->amcanbackward`.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct AmCaps: u32 {
        const ORDER             = 1 << 0;  // amcanorder
        const ORDER_BY_OP       = 1 << 1;  // amcanorderbyop
        const HASH              = 1 << 2;  // amcanhash
        const CONSISTENT_EQ     = 1 << 3;  // amconsistentequality
        const CONSISTENT_ORD    = 1 << 4;  // amconsistentordering
        const BACKWARD          = 1 << 5;  // amcanbackward
        const UNIQUE            = 1 << 6;  // amcanunique
        const MULTICOL          = 1 << 7;  // amcanmulticol
        const OPTIONAL_KEY      = 1 << 8;  // amoptionalkey
        const SEARCH_ARRAY      = 1 << 9;  // amsearcharray
        const SEARCH_NULLS      = 1 << 10; // amsearchnulls
        const STORAGE           = 1 << 11; // amstorage
        const CLUSTERABLE       = 1 << 12; // amclusterable
        const PRED_LOCKS        = 1 << 13; // ampredlocks
        const PARALLEL          = 1 << 14; // amcanparallel
        const BUILD_PARALLEL    = 1 << 15; // amcanbuildparallel
        const INCLUDE           = 1 << 16; // amcaninclude
        const USE_MAINT_WORKMEM = 1 << 17; // amusemaintenanceworkmem
        const SUMMARIZING       = 1 << 18; // amsummarizing
    }
}

bitflags! {
    /// `amparallelvacuumoptions`: a flag word (the VACUUM_OPTION_* parallel bits,
    /// see commands/vacuum.h). Stored as the `u8` it is in C.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct AmParallelVacuumOptions: u8 {
        const PARALLEL_BULKDEL          = 1 << 0;
        const PARALLEL_COND_CLEANUP     = 1 << 1;
        const PARALLEL_CLEANUP          = 1 << 2;
    }
}

/// The six built-in index AMs, resolved at runtime in C via
/// `pg_class.relam -> pg_am.amhandler`. A closed enum gives static dispatch: in
/// each arm the concrete type is known so its capability supertraits are in
/// scope (the Btree arm can call MarkRestore/PlainScan/BitmapScan; the Brin arm
/// only BitmapScan). Extension AMs (contrib/bloom, out-of-tree) are the open
/// case handled by a separate fn-pointer fallback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexAmKind {
    Btree,
    Hash,
    Gist,
    Gin,
    SpGist,
    Brin,
}

/// Base trait for an index access method (C `IndexAmRoutine`). The required
/// callbacks plus the begin-scan factory. Non-flag scalar fields
/// (`amstrategies`, `amsupport`, `amoptsprocnum`, `amkeytype`) are per-AM
/// associated consts. Lone optional non-scan callbacks (`aminsertcleanup`,
/// `amgettreeheight`, `amproperty`, `ambuildphasename`, `amadjustmembers`) are
/// provided default methods.
pub trait IndexAm {
    /// Per-scan handle; borrows the index relation (GAT). `amendscan` is its Drop.
    type Scan<'a>: IndexScan
    where
        Self: 'a;

    /// amstrategies: total number of strategies, or 0 if no fixed set.
    const STRATEGIES: u16;
    /// amsupport: total number of support functions.
    const SUPPORT: u16;
    /// amoptsprocnum: opclass options support function number, or 0.
    const OPTSPROCNUM: u16;
    /// amkeytype: type of data stored in index, or InvalidOid if variable.
    const KEYTYPE: Oid;

    /// The `amcan*` capability bits (Section 2).
    fn capabilities(&self) -> AmCaps;

    /// amparallelvacuumoptions: OR of parallel vacuum flags.
    fn parallel_vacuum_options(&self) -> AmParallelVacuumOptions {
        AmParallelVacuumOptions::empty()
    }

    // --- Build & maintenance (required) ---

    /// ambuild: build a new index.
    fn build(&self, heap: &Relation, index: &Relation, info: &IndexInfo) -> IndexBuildResult;

    /// ambuildempty: build an empty index.
    fn build_empty(&self, index: &Relation);

    /// aminsert: insert this tuple. Returns false if a unique-check conflict was
    /// recorded but not raised, true otherwise (matches C's bool).
    #[allow(clippy::too_many_arguments)]
    fn insert(
        &self,
        index: &Relation,
        values: &[Datum],
        isnull: &[bool],
        heap_tid: &ItemPointerData,
        heap: &Relation,
        check: IndexUniqueCheck,
        index_unchanged: bool,
        info: &IndexInfo,
    ) -> bool;

    /// ambulkdelete: bulk delete index entries. The C `void *callback_state`
    /// opaque context is captured by the callback closure.
    fn bulk_delete(
        &self,
        info: &IndexVacuumInfo,
        stats: Option<Box<IndexBulkDeleteResult>>,
        callback: &mut IndexBulkDeleteCallback,
    ) -> Box<IndexBulkDeleteResult>;

    /// amvacuumcleanup: post-VACUUM cleanup. None when no stats are produced.
    fn vacuum_cleanup(
        &self,
        info: &IndexVacuumInfo,
        stats: Option<Box<IndexBulkDeleteResult>>,
    ) -> Option<Box<IndexBulkDeleteResult>>;

    /// amcostestimate: estimate cost of an indexscan. The four C out-params
    /// (startup/total cost, selectivity, correlation, pages) collapse into the
    /// returned struct.
    fn cost_estimate(
        &self,
        root: &PlannerInfo,
        path: &mut IndexPath,
        loop_count: f64,
    ) -> IndexCostEstimate;

    /// amoptions: parse index reloptions. None means use defaults.
    fn options(&self, reloptions: Datum, validate: bool) -> Option<*mut bytea>;

    /// amvalidate: validate the definition of an opclass for this AM.
    fn validate(&self, opclassoid: Oid) -> bool;

    /// ambeginscan: prepare for an index scan (the scan factory).
    fn begin_scan<'a>(&'a self, index: &'a Relation, nkeys: i32, norderbys: i32) -> Self::Scan<'a>;

    // --- Optional non-scan callbacks (NULL-checked in C) -> default methods ---

    /// aminsertcleanup: cleanup after insert. No-op by default.
    fn insert_cleanup(&self, _index: &Relation, _info: &IndexInfo) {}

    /// amgettreeheight: height of a tree-structured index (used by cost_estimate).
    fn get_tree_height(&self, _rel: &Relation) -> i32 {
        0
    }

    /// amproperty: report an AM/index/column property. None means "not handled
    /// here, fall back to core defaults" (the C bool-return + out-params).
    fn property(
        &self,
        _index_oid: Oid,
        _attno: i32,
        _prop: IndexAMProperty,
        _propname: &str,
    ) -> Option<Option<bool>> {
        None
    }

    /// ambuildphasename: name of a build phase for progress reporting.
    fn build_phase_name(&self, _phasenum: i64) -> Option<String> {
        None
    }

    /// amadjustmembers: validate operators/support funcs added to an opclass.
    fn adjust_members(
        &self,
        _opfamilyoid: Oid,
        _opclassoid: Oid,
        _operators: &mut [OpFamilyMember],
        _functions: &mut [OpFamilyMember],
    ) {
    }
}

/// Result of `IndexAm::cost_estimate` (C's four amcostestimate out-params).
pub struct IndexCostEstimate {
    pub index_startup_cost: f64, // Cost
    pub index_total_cost: f64,   // Cost
    pub index_selectivity: f64,  // Selectivity
    pub index_correlation: f64,
    pub index_pages: f64,
}

/// A live index scan handle (C `IndexScanDesc`). `ambeginscan`/`amrescan` map
/// here; `amendscan` is `Drop`. Per-scan state that C keeps in
/// `IndexScanDesc.opaque` lives in the concrete implementor.
pub trait IndexScan {
    /// amrescan: (re)start the scan with new keys/orderbys.
    fn rescan(&mut self, keys: &mut [ScanKeyData], orderbys: &mut [ScanKeyData]);
}

/// amgettuple -- tuple-at-a-time scan. NULL in BRIN. Returns the next matching
/// heap TID, or None when the scan is exhausted (C bool + scan->xs_heaptid).
pub trait PlainScan: IndexScan {
    fn get_tuple(&mut self, dir: ScanDirection) -> Option<ItemPointerData>;
}

/// amgetbitmap -- fetch all valid tuples into a bitmap; returns the count.
pub trait BitmapScan: IndexScan {
    fn get_bitmap(&mut self, tbm: &mut TIDBitmap) -> i64;
}

/// ammarkpos/amrestrpos -- only for ordered tuple-at-a-time scans, hence a
/// supertrait of PlainScan, not IndexScan.
pub trait MarkRestore: PlainScan {
    fn mark_pos(&mut self);
    fn restore_pos(&mut self);
}

/// amcanreturn -- supports index-only scans.
pub trait CanReturn: IndexAm {
    fn can_return(&self, index: &Relation, attno: i32) -> bool;
}

/// The amestimateparallelscan group (all-or-none).
pub trait ParallelIndexScan: IndexAm {
    /// amestimateparallelscan: size of the parallel scan descriptor.
    fn estimate_parallel_scan(&self, nkeys: i32, norderbys: i32) -> usize;
    /// aminitparallelscan: initialize shared parallel scan state.
    fn init_parallel_scan(&self, target: &mut ParallelIndexScanDescData);
    /// amparallelrescan: (re)start a parallel scan.
    fn parallel_rescan(&self, scan: &mut Self::Scan<'_>);
}

/// amtranslatestrategy/amtranslatecmptype -- planning support.
pub trait Translate: IndexAm {
    fn translate_strategy(&self, strategy: StrategyNumber, opfamily: Oid) -> CompareType;
    fn translate_cmptype(&self, cmptype: CompareType, opfamily: Oid) -> StrategyNumber;
}

// --- Functions in access/index/amapi.c ---

/// Look up an index AM's routine from its handler function OID.
pub fn GetIndexAmRoutine(_amhandler: Oid) -> IndexAmKind {
    unimplemented!()
}

/// Look up an index AM's routine by AM OID; None on miss when `noerror`.
pub fn GetIndexAmRoutineByAmId(_amoid: Oid, _noerror: bool) -> Option<IndexAmKind> {
    unimplemented!()
}

/// Translate an AM-specific strategy to a CompareType. None on miss when
/// `missing_ok` (C's CompareType + missing_ok).
pub fn IndexAmTranslateStrategy(
    _strategy: StrategyNumber,
    _amoid: Oid,
    _opfamily: Oid,
    _missing_ok: bool,
) -> Option<CompareType> {
    unimplemented!()
}

/// Translate a CompareType to an AM-specific strategy. None on miss when
/// `missing_ok`.
pub fn IndexAmTranslateCompareType(
    _cmptype: CompareType,
    _amoid: Oid,
    _opfamily: Oid,
    _missing_ok: bool,
) -> Option<StrategyNumber> {
    unimplemented!()
}
