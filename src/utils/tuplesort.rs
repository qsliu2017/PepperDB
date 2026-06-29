//! Header for PostgreSQL src/include/utils/tuplesort.h.
//!
//! The generalized tuple sort (in-memory qsort + bounded top-N heap + the
//! external-merge state machine) is translated in
//! `crate::backend::utils::sort::tuplesort` (step 24). This header re-exports the
//! implementation under the C-facing path so existing `crate::utils::tuplesort::*`
//! call sites keep resolving. `Tuplesortstate` is now an owned struct (genuinely
//! `Send`), holding an owned `Vec<SortTuple>` rather than the former opaque stub.
//!
//! The parallel/shared-sort coordination types (`Sharedsort`, `SortCoordinate`)
//! and the reporting structs are kept here as staged types: the backend does not
//! yet implement parallel sort (single-process), and instrumentation reporting is
//! deferred. They have no backend equivalent and carry no pointers.

pub use crate::backend::utils::sort::tuplesort::{
    sortopt, tuplesort_begin_datum, tuplesort_begin_heap, tuplesort_end, tuplesort_getdatum,
    tuplesort_gettupleslot, tuplesort_markpos, tuplesort_performsort, tuplesort_putdatum,
    tuplesort_puttupleslot, tuplesort_rescan, tuplesort_restorepos, tuplesort_set_bound,
    tuplesort_skiptuples, tuplesort_used_bound, AttrNumber, SortKey, SortTuple, SortTupleBody,
    Tuplesortstate,
};

/// Sort algorithm used, for reporting sort statistics (staged -- instrumentation
/// reporting is deferred). In C these are OR-able bit values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TuplesortMethod {
    StillInProgress = 0,
    TopNHeapsort = 1 << 0,
    Quicksort = 1 << 1,
    ExternalSort = 1 << 2,
    ExternalMerge = 1 << 3,
}

/// Type of space `spaceUsed` represents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TuplesortSpaceType {
    Disk = 0,
    Memory,
}

/// Reporting struct for sort statistics (no pointers; shared-mem safe).
#[derive(Debug, Clone, Copy)]
pub struct TuplesortInstrumentation {
    pub sortMethod: TuplesortMethod,
    pub spaceType: TuplesortSpaceType,
    pub spaceUsed: i64,
}

// Sharedsort is parallel-sort shared state; staged (single-process collapses the
// shmem coordination). Kept opaque until parallel sort is translated.
pub struct Sharedsort {
    _private: (),
}

/// Tuplesort parallel coordination state (staged with `Sharedsort`).
pub struct SortCoordinateData {
    pub isWorker: bool,
    pub nParticipants: i32,
    pub sharedsort: Option<Box<Sharedsort>>,
}
