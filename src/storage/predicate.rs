//! Translated from PostgreSQL src/include/storage/predicate.h
//!
//! STUB (foundation-rewrite: lock-manager). The SSI predicate-lock public API.
//! All bodies are `// TODO(lock-manager)`. Backed by
//! `crate::storage::predicate_internals` in Phase 2.

use crate::c::{Size, TransactionId};
use crate::storage::block::BlockNumber;
use crate::storage::itemptr::ItemPointerData;
use crate::storage::lock::VirtualTransactionId;
use crate::utils::relcache::Relation;
use crate::utils::snapshot::Snapshot;

// GUC variables -> process-globals, deferred to Session/GUC state (Phase 2).
pub static mut max_predicate_locks_per_xact: i32 = 0;
pub static mut max_predicate_locks_per_relation: i32 = 0;
pub static mut max_predicate_locks_per_page: i32 = 0;

// A handle sharing SERIALIZABLEXACT objects between parallel-query participants.
// C `void *`; opaque owned handle in Phase 2.
pub type SerializableXactHandle = *mut core::ffi::c_void; // TODO(ptr)

// housekeeping for shared-memory predicate lock structures (shmem collapses)
pub fn PredicateLockShmemInit() {
    unimplemented!() // TODO(lock-manager)
}
pub fn PredicateLockShmemSize() -> Size {
    unimplemented!() // TODO(lock-manager)
}

pub fn CheckPointPredicate() {
    unimplemented!() // TODO(lock-manager)
}

// predicate lock reporting
pub fn PageIsPredicateLocked(_relation: Relation, _blkno: BlockNumber) -> bool {
    unimplemented!() // TODO(lock-manager)
}

// predicate lock maintenance
pub fn GetSerializableTransactionSnapshot(_snapshot: Snapshot) -> Snapshot<'static> {
    unimplemented!() // TODO(lock-manager)
}
pub fn SetSerializableTransactionSnapshot(
    _snapshot: Snapshot,
    _sourcevxid: &VirtualTransactionId,
    _sourcepid: i32,
) {
    unimplemented!() // TODO(lock-manager)
}
pub fn RegisterPredicateLockingXid(_xid: TransactionId) {
    unimplemented!() // TODO(lock-manager)
}
pub fn PredicateLockRelation(_relation: Relation, _snapshot: Snapshot) {
    unimplemented!() // TODO(lock-manager)
}
pub fn PredicateLockPage(_relation: Relation, _blkno: BlockNumber, _snapshot: Snapshot) {
    unimplemented!() // TODO(lock-manager)
}
pub fn PredicateLockTID(
    _relation: Relation,
    _tid: &ItemPointerData,
    _snapshot: Snapshot,
    _tuple_xid: TransactionId,
) {
    unimplemented!() // TODO(lock-manager)
}
pub fn PredicateLockPageSplit(
    _relation: Relation,
    _oldblkno: BlockNumber,
    _newblkno: BlockNumber,
) {
    unimplemented!() // TODO(lock-manager)
}
pub fn PredicateLockPageCombine(
    _relation: Relation,
    _oldblkno: BlockNumber,
    _newblkno: BlockNumber,
) {
    unimplemented!() // TODO(lock-manager)
}
pub fn TransferPredicateLocksToHeapRelation(_relation: Relation) {
    unimplemented!() // TODO(lock-manager)
}
pub fn ReleasePredicateLocks(_is_commit: bool, _is_read_only_safe: bool) {
    unimplemented!() // TODO(lock-manager)
}

// conflict detection (may also trigger rollback)
pub fn CheckForSerializableConflictOutNeeded(_relation: Relation, _snapshot: Snapshot) -> bool {
    unimplemented!() // TODO(lock-manager)
}
pub fn CheckForSerializableConflictOut(
    _relation: Relation,
    _xid: TransactionId,
    _snapshot: Snapshot,
) {
    unimplemented!() // TODO(lock-manager)
}
pub fn CheckForSerializableConflictIn(
    _relation: Relation,
    _tid: &ItemPointerData,
    _blkno: BlockNumber,
) {
    unimplemented!() // TODO(lock-manager)
}
pub fn CheckTableForSerializableConflictIn(_relation: Relation) {
    unimplemented!() // TODO(lock-manager)
}

// final rollback checking
pub fn PreCommit_CheckForSerializationFailure() {
    unimplemented!() // TODO(lock-manager)
}

// two-phase commit support
pub fn AtPrepare_PredicateLocks() {
    unimplemented!() // TODO(lock-manager)
}
pub fn PostPrepare_PredicateLocks(_xid: TransactionId) {
    unimplemented!() // TODO(lock-manager)
}
pub fn PredicateLockTwoPhaseFinish(_xid: TransactionId, _is_commit: bool) {
    unimplemented!() // TODO(lock-manager)
}
pub fn predicatelock_twophase_recover(_xid: TransactionId, _info: u16, _recdata: &[u8]) {
    unimplemented!() // TODO(lock-manager)
}

// parallel query support
pub fn ShareSerializableXact() -> SerializableXactHandle {
    unimplemented!() // TODO(lock-manager)
}
pub fn AttachSerializableXact(_handle: SerializableXactHandle) {
    unimplemented!() // TODO(lock-manager)
}
