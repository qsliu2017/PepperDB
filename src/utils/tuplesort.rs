//! Translated from PostgreSQL src/include/utils/tuplesort.h
//! Generalized tuple sorting routines (in-memory qsort + external merge sort).

use bitflags::bitflags;

use crate::access::brin_tuple::BrinTuple;
use crate::access::gin_tuple::GinTuple;
use crate::access::htup::HeapTuple;
use crate::access::itup::IndexTuple;
use crate::access::tupdesc::TupleDesc;
use crate::executor::tuptable::TupleTableSlot;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::storage::itemptr::ItemPointerData;
use crate::utils::logtape::LogicalTape;
use crate::utils::sortsupport::SortSupport;
use std::sync::Arc;
use crate::utils::rel::RelationData;

pub type AttrNumber = i16; // c.h AttrNumber
pub type ItemPointer = *mut ItemPointerData; // TODO(ptr): non-null ItemPointer

/// Tuplesortstate and Sharedsort are opaque; details live in tuplesort.c.
pub struct Tuplesortstate {
    _private: (),
}

// Sharedsort is parallel-sort shared state; shmem collapses under single-process.
pub struct Sharedsort {
    _private: (),
}

/// Tuplesort parallel coordination state, allocated by each participant.
pub struct SortCoordinateData {
    /// Worker process? If not, must be leader.
    pub isWorker: bool,
    /// Leader-passed count of participants launched (workers set this to -1).
    pub nParticipants: i32,
    /// Private opaque state (shared-memory pointer in C). TODO(ptr): Arc in Phase 2.
    pub sharedsort: Option<Box<Sharedsort>>,
}

pub type SortCoordinate<'a> = &'a mut SortCoordinateData;

/// Sort algorithm used, for reporting sort statistics.
///
/// Note: in C these are OR-able bit values (1<<n) because the parallel-sort
/// infrastructure combines per-worker methods into one word, with
/// SORT_TYPE_STILL_IN_PROGRESS = 0 meaning "worker did nothing". Modeled here as
/// an enum per the type plan; combining is handled at the (single) call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TuplesortMethod {
    StillInProgress = 0,
    TopNHeapsort = 1 << 0,
    Quicksort = 1 << 1,
    ExternalSort = 1 << 2,
    ExternalMerge = 1 << 3,
}

pub const NUM_TUPLESORTMETHODS: i32 = 4;

/// Type of space `spaceUsed` represents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TuplesortSpaceType {
    Disk = 0,
    Memory,
}

bitflags! {
    /// Bitwise option flags for tuple sorts (sortopt). TUPLESORT_NONE = empty().
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct TuplesortFlags: i32 {
        /// Non-sequential access to the sort result is required.
        const RANDOMACCESS = 1 << 0;
        /// The tuplesort is able to support bounded sorts.
        const ALLOWBOUNDED = 1 << 1;
    }
}

impl TuplesortFlags {
    pub const NONE: Self = Self::empty();
}

/// True when a bump allocator can be used for tuple allocation (no bounded sort).
pub const fn TupleSortUseBumpTupleCxt(opt: TuplesortFlags) -> bool {
    !opt.contains(TuplesortFlags::ALLOWBOUNDED)
}

/// Reporting struct for sort statistics. Contains no pointers (shared-mem safe).
#[derive(Debug, Clone, Copy)]
pub struct TuplesortInstrumentation {
    pub sortMethod: TuplesortMethod,    // sort algorithm used
    pub spaceType: TuplesortSpaceType,  // type of space spaceUsed represents
    pub spaceUsed: i64,                 // space consumption, in kB
}

/// The objects we actually sort. `tuple` is a separate palloc chunk (MinimalTuple
/// or IndexTuple); datum1/isnull1 hold the first key column (or an abbreviated key).
pub struct SortTuple {
    pub tuple: *mut core::ffi::c_void, // TODO(ptr): MinimalTuple/IndexTuple, points into arena
    pub datum1: Datum,                 // value of first key column
    pub isnull1: bool,                 // is first key column NULL?
    pub srctape: i32,                  // source tape number
}

/// Comparator; result per qsort() convention (<0, 0, >0 for a<b, a=b, a>b).
/// C `void *arg` (the state) is captured by the closure.
pub type SortTupleComparator<'a> = Box<dyn Fn(&SortTuple, &SortTuple) -> i32 + 'a>;

// The public part of a tuplesort (TuplesortPublic) is a struct of fn pointers
// (vtable) selecting the sort variant, plus shared implementation fields. Per
// routine-struct/function-mapping, model the dispatch as a trait; the variant
// callbacks (comparetup/removeabbrev/writetup/readtup/freestate) become methods.
pub trait TuplesortVariant {
    /// Compare two tuples; result per qsort() convention.
    fn comparetup(&self, state: &Tuplesortstate, a: &SortTuple, b: &SortTuple) -> i32;
    /// Fall back to the full tuple; compares first sortkey only if abbreviated.
    fn comparetup_tiebreak(&self, state: &Tuplesortstate, a: &SortTuple, b: &SortTuple) -> i32;
    /// Restore datum1 from the abbreviated key back to the first column value.
    fn removeabbrev(&self, state: &mut Tuplesortstate, stups: &mut [SortTuple]);
    /// Write a stored tuple onto tape (on-tape form may differ from in memory).
    fn writetup(&self, state: &mut Tuplesortstate, tape: &mut LogicalTape, stup: &SortTuple);
    /// Read a stored tuple from tape; `len` is the already-read length.
    fn readtup(&self, state: &mut Tuplesortstate, stup: &mut SortTuple, tape: &mut LogicalTape, len: u32);
    /// Release sort-variant-specific resources (the C `arg` field). Optional.
    fn freestate(&self, _state: &mut Tuplesortstate) {}
}

/// Public part of a tuplesort state (the implementation-shared fields). The C
/// vtable fn pointers are split out into `TuplesortVariant`.
pub struct TuplesortPublic<'a> {
    pub variant: Box<dyn TuplesortVariant>, // comparetup/writetup/... vtable

    pub maincontext: crate::utils::palloc::MemoryContext, // persists across batches
    pub sortcontext: crate::utils::palloc::MemoryContext, // most sort data
    pub tuplecontext: crate::utils::palloc::MemoryContext, // sub-context for tuple data

    /// Whether SortTuple's datum1/isnull1 are maintained by the variant routines.
    pub haveDatum1: bool,

    pub nKeys: i32,                          // number of columns in sort key
    pub sortKeys: Option<SortSupport<'a>>,   // array of length nKeys
    /// Set for single-key MinimalTuple and Datum cases (both use qsort_ssup).
    pub onlyKey: Option<SortSupport<'a>>,

    pub sortopt: TuplesortFlags,             // flags used to set up the sort
    pub tuples: bool,                        // can SortTuple.tuple ever be set?
    // C `void *arg` is folded into the `variant` trait object's own state.
}

/// PARALLEL_SORT probe code: 0 = serial, 1 = worker, 2 = leader.
pub fn PARALLEL_SORT(coordinate: Option<&SortCoordinateData>) -> i32 {
    match coordinate {
        None => 0,
        Some(c) if c.sharedsort.is_none() => 0,
        Some(c) if c.isWorker => 1,
        Some(_) => 2,
    }
}

/// C: `(TuplesortPublic *) state` - the public part is the prefix of the state.
pub fn TuplesortstateGetPublic(_state: &Tuplesortstate) -> &TuplesortPublic<'_> {
    unimplemented!()
}

// LogicalTapeReadExact: read exactly `len` bytes or elog(ERROR). The fallible read
// maps to Result/? in Phase 2; kept as a free fn here.
#[deprecated(note = "TODO(panic): migrate to Result + ?")]
pub fn LogicalTapeReadExact(_tape: &mut LogicalTape, _ptr: &mut [u8], _len: usize) {
    unimplemented!() // TODO(panic)
}

// === core tuplesort.c API ===

pub fn tuplesort_begin_common(
    _workMem: i32,
    _coordinate: Option<SortCoordinate>,
    _sortopt: TuplesortFlags,
) -> Box<Tuplesortstate> {
    unimplemented!()
}

pub fn tuplesort_set_bound(_state: &mut Tuplesortstate, _bound: i64) {
    unimplemented!()
}

pub fn tuplesort_used_bound(_state: &Tuplesortstate) -> bool {
    unimplemented!()
}

pub fn tuplesort_puttuple_common(
    _state: &mut Tuplesortstate,
    _tuple: &SortTuple,
    _useAbbrev: bool,
    _tuplen: usize,
) {
    unimplemented!()
}

pub fn tuplesort_performsort(_state: &mut Tuplesortstate) {
    unimplemented!()
}

// returns false at end of data; out-param `stup` filled when true -> Option.
pub fn tuplesort_gettuple_common(
    _state: &mut Tuplesortstate,
    _forward: bool,
) -> Option<SortTuple> {
    unimplemented!()
}

pub fn tuplesort_skiptuples(_state: &mut Tuplesortstate, _ntuples: i64, _forward: bool) -> bool {
    unimplemented!()
}

pub fn tuplesort_end(_state: &mut Tuplesortstate) {
    unimplemented!()
}

pub fn tuplesort_reset(_state: &mut Tuplesortstate) {
    unimplemented!()
}

// out-param `stats` -> return value.
pub fn tuplesort_get_stats(_state: &mut Tuplesortstate) -> TuplesortInstrumentation {
    unimplemented!()
}

pub fn tuplesort_method_name(_m: TuplesortMethod) -> &'static str {
    unimplemented!()
}

pub fn tuplesort_space_type_name(_t: TuplesortSpaceType) -> &'static str {
    unimplemented!()
}

pub fn tuplesort_merge_order(_allowedMem: i64) -> i32 {
    unimplemented!()
}

pub fn tuplesort_estimate_shared(_nWorkers: i32) -> usize {
    unimplemented!()
}

// dsm_segment is tombstoned (single-process); drop the seg argument.
pub fn tuplesort_initialize_shared(_shared: &mut Sharedsort, _nWorkers: i32) {
    unimplemented!()
}

pub fn tuplesort_attach_shared(_shared: &mut Sharedsort) {
    unimplemented!()
}

// === random-access-only (require TUPLESORT_RANDOMACCESS) ===

pub fn tuplesort_rescan(_state: &mut Tuplesortstate) {
    unimplemented!()
}

pub fn tuplesort_markpos(_state: &mut Tuplesortstate) {
    unimplemented!()
}

pub fn tuplesort_restorepos(_state: &mut Tuplesortstate) {
    unimplemented!()
}

pub fn tuplesort_readtup_alloc(_state: &mut Tuplesortstate, _tuplen: usize) -> *mut core::ffi::c_void {
    unimplemented!() // TODO(ptr): slab/palloc'd buffer
}

// === tuplesortvariants.c ===

pub fn tuplesort_begin_heap(
    _tupDesc: TupleDesc,
    _nkeys: i32,
    _attNums: &[AttrNumber],
    _sortOperators: &[Oid],
    _sortCollations: &[Oid],
    _nullsFirstFlags: &[bool],
    _workMem: i32,
    _coordinate: Option<SortCoordinate>,
    _sortopt: TuplesortFlags,
) -> Box<Tuplesortstate> {
    unimplemented!()
}

pub fn tuplesort_begin_cluster(
    _tupDesc: TupleDesc,
    _indexRel: &RelationData,
    _workMem: i32,
    _coordinate: Option<SortCoordinate>,
    _sortopt: TuplesortFlags,
) -> Box<Tuplesortstate> {
    unimplemented!()
}

pub fn tuplesort_begin_index_btree(
    _heapRel: &RelationData,
    _indexRel: &RelationData,
    _enforceUnique: bool,
    _uniqueNullsNotDistinct: bool,
    _workMem: i32,
    _coordinate: Option<SortCoordinate>,
    _sortopt: TuplesortFlags,
) -> Box<Tuplesortstate> {
    unimplemented!()
}

pub fn tuplesort_begin_index_hash(
    _heapRel: &RelationData,
    _indexRel: &RelationData,
    _high_mask: u32,
    _low_mask: u32,
    _max_buckets: u32,
    _workMem: i32,
    _coordinate: Option<SortCoordinate>,
    _sortopt: TuplesortFlags,
) -> Box<Tuplesortstate> {
    unimplemented!()
}

pub fn tuplesort_begin_index_gist(
    _heapRel: &RelationData,
    _indexRel: &RelationData,
    _workMem: i32,
    _coordinate: Option<SortCoordinate>,
    _sortopt: TuplesortFlags,
) -> Box<Tuplesortstate> {
    unimplemented!()
}

pub fn tuplesort_begin_index_brin(
    _workMem: i32,
    _coordinate: Option<SortCoordinate>,
    _sortopt: TuplesortFlags,
) -> Box<Tuplesortstate> {
    unimplemented!()
}

pub fn tuplesort_begin_index_gin(
    _heapRel: &RelationData,
    _indexRel: &RelationData,
    _workMem: i32,
    _coordinate: Option<SortCoordinate>,
    _sortopt: TuplesortFlags,
) -> Box<Tuplesortstate> {
    unimplemented!()
}

pub fn tuplesort_begin_datum(
    _datumType: Oid,
    _sortOperator: Oid,
    _sortCollation: Oid,
    _nullsFirstFlag: bool,
    _workMem: i32,
    _coordinate: Option<SortCoordinate>,
    _sortopt: TuplesortFlags,
) -> Box<Tuplesortstate> {
    unimplemented!()
}

pub fn tuplesort_puttupleslot(_state: &mut Tuplesortstate, _slot: &TupleTableSlot) {
    unimplemented!()
}

pub fn tuplesort_putheaptuple(_state: &mut Tuplesortstate, _tup: HeapTuple) {
    unimplemented!()
}

pub fn tuplesort_putindextuplevalues(
    _state: &mut Tuplesortstate,
    _rel: &RelationData,
    _self_: ItemPointer,
    _values: &[Datum],
    _isnull: &[bool],
) {
    unimplemented!()
}

pub fn tuplesort_putbrintuple(_state: &mut Tuplesortstate, _tuple: &BrinTuple, _size: usize) {
    unimplemented!()
}

pub fn tuplesort_putgintuple(_state: &mut Tuplesortstate, _tuple: &GinTuple, _size: usize) {
    unimplemented!()
}

pub fn tuplesort_putdatum(_state: &mut Tuplesortstate, _val: Datum, _isNull: bool) {
    unimplemented!()
}

// returns false at end; `abbrev` out-param filled alongside.
pub fn tuplesort_gettupleslot(
    _state: &mut Tuplesortstate,
    _forward: bool,
    _copy: bool,
    _slot: &mut TupleTableSlot,
    _abbrev: Option<&mut Datum>,
) -> bool {
    unimplemented!()
}

// invalid tuple sentinel at end-of-data -> Option.
pub fn tuplesort_getheaptuple(_state: &mut Tuplesortstate, _forward: bool) -> Option<HeapTuple> {
    unimplemented!()
}

pub fn tuplesort_getindextuple(_state: &mut Tuplesortstate, _forward: bool) -> Option<IndexTuple> {
    unimplemented!()
}

// out-param `len` returned alongside the tuple; None at end-of-data.
pub fn tuplesort_getbrintuple(_state: &mut Tuplesortstate, _forward: bool) -> Option<(*mut BrinTuple, usize)> {
    unimplemented!()
}

pub fn tuplesort_getgintuple(_state: &mut Tuplesortstate, _forward: bool) -> Option<(*mut GinTuple, usize)> {
    unimplemented!()
}

// returns false at end; out-params val/isNull/abbrev folded into Some(...).
pub fn tuplesort_getdatum(
    _state: &mut Tuplesortstate,
    _forward: bool,
    _copy: bool,
    _abbrev: Option<&mut Datum>,
) -> Option<(Datum, bool)> {
    unimplemented!()
}
