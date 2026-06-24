//! Translated from PostgreSQL src/include/utils/sharedtuplestore.h
//! Simple mechanism for sharing tuples between backends.
//! Single-process model: the shared-memory backing collapses to owned heap state.

use crate::access::htup::MinimalTupleData;
use crate::storage::sharedfileset::SharedFileSet;
use bitflags::bitflags;

// HeapTuple-family pointer alias (htup.h does not export it yet). TODO(ptr)
pub type MinimalTuple = *mut MinimalTupleData;

/// Opaque shared state describing the tuplestore.
pub struct SharedTuplestore {
    _private: [u8; 0],
}

/// Per-participant accessor handle.
pub struct SharedTuplestoreAccessor {
    _private: [u8; 0],
}

bitflags! {
    /// sts_initialize flags.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct StsFlags: i32 {
        /// Scanned only once, so backing files can be unlinked early.
        const SINGLE_PASS = 0x01;
    }
}

pub fn sts_estimate(_participants: i32) -> usize {
    unimplemented!()
}

pub fn sts_initialize(
    _sts: &mut SharedTuplestore,
    _participants: i32,
    _my_participant_number: i32,
    _meta_data_size: usize,
    _flags: StsFlags,
    _fileset: &mut SharedFileSet,
    _name: &str,
) -> *mut SharedTuplestoreAccessor {
    unimplemented!()
}

pub fn sts_attach(
    _sts: &mut SharedTuplestore,
    _my_participant_number: i32,
    _fileset: &mut SharedFileSet,
) -> *mut SharedTuplestoreAccessor {
    unimplemented!()
}

pub fn sts_end_write(_accessor: &mut SharedTuplestoreAccessor) {
    unimplemented!()
}

pub fn sts_reinitialize(_accessor: &mut SharedTuplestoreAccessor) {
    unimplemented!()
}

pub fn sts_begin_parallel_scan(_accessor: &mut SharedTuplestoreAccessor) {
    unimplemented!()
}

pub fn sts_end_parallel_scan(_accessor: &mut SharedTuplestoreAccessor) {
    unimplemented!()
}

pub fn sts_puttuple(
    _accessor: &mut SharedTuplestoreAccessor,
    _meta_data: &[u8],
    _tuple: MinimalTuple,
) {
    unimplemented!()
}

/// Returns the next tuple, or None at end of this participant's scan. The
/// per-tuple meta_data is written into `meta_data`.
pub fn sts_parallel_scan_next(
    _accessor: &mut SharedTuplestoreAccessor,
    _meta_data: &mut [u8],
) -> Option<MinimalTuple> {
    unimplemented!()
}
