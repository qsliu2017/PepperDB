//! Translated from PostgreSQL src/include/replication/conflict.h
//! Exports for conflicts logging (logical replication apply).

use crate::access::xlogdefs::RepOriginId;
use crate::c::TransactionId;
use crate::datatype::timestamp::TimestampTz;
use crate::postgres_ext::Oid;

/// Conflict types that could occur while applying remote changes.
/// Also used in statistics collection; preserve order when extending.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictType {
    /// The row to be inserted violates a unique constraint.
    InsertExists,
    /// The row to be updated was modified by a different origin.
    UpdateOriginDiffers,
    /// The updated row value violates a unique constraint.
    UpdateExists,
    /// The row to be updated is missing.
    UpdateMissing,
    /// The row to be deleted was modified by a different origin.
    DeleteOriginDiffers,
    /// The row to be deleted is missing.
    DeleteMissing,
    /// The row to be inserted/updated violates multiple unique constraints.
    MultipleUniqueConflicts,
}

pub const CONFLICT_NUM_TYPES: usize = ConflictType::MultipleUniqueConflicts as usize + 1;

// TODO(struct-forward): EState/ResultRelInfo/TupleTableSlot are defined in
// nodes/execnodes.h and executor/tuptable.h; repoint in Phase 2.
#[deprecated(note = "TODO(struct-forward): repoint to crate::nodes::execnodes::EState in Phase 2")]
pub struct EState;
#[deprecated(note = "TODO(struct-forward): repoint to crate::nodes::execnodes::ResultRelInfo in Phase 2")]
pub struct ResultRelInfo;
#[deprecated(note = "TODO(struct-forward): repoint to crate::executor::tuptable::TupleTableSlot in Phase 2")]
pub struct TupleTableSlot;

/// Information for the existing local row that caused the conflict.
#[allow(deprecated)]
pub struct ConflictTupleInfo {
    pub slot: Box<TupleTableSlot>, // tuple slot holding the conflicting local tuple
    pub indexoid: Oid,             // OID of the index where the conflict occurred
    pub xmin: TransactionId,       // xid of the modification causing the conflict
    pub origin: RepOriginId,       // origin of the modification
    pub ts: TimestampTz,           // timestamp of the conflicting modification
}

/// bool + (`*xmin`, `*localorigin`, `*localts`) out-params -> Option of a tuple.
#[allow(deprecated)]
pub fn get_tuple_transaction_info(
    _localslot: &TupleTableSlot,
) -> Option<(TransactionId, RepOriginId, TimestampTz)> {
    unimplemented!()
}

#[allow(deprecated)]
pub fn report_apply_conflict(
    _estate: &mut EState,
    _relinfo: &mut ResultRelInfo,
    _elevel: i32,
    _type: ConflictType,
    _searchslot: &mut TupleTableSlot,
    _remoteslot: &mut TupleTableSlot,
    _conflicttuples: Vec<ConflictTupleInfo>,
) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn init_conflict_indexes(_rel_info: &mut ResultRelInfo) {
    unimplemented!()
}
