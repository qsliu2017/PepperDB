//! Translated from PostgreSQL src/include/storage/lmgr.h

// High-level lock manager API (LOCKTAG-based wrappers over storage::lock).
// TODO(lock-manager): sharded tables, async waits

use crate::postgres_ext::Oid;
use crate::storage::block::BlockNumber;
use crate::storage::itemptr::ItemPointerData;
use crate::storage::lock::{LOCKMODE, LOCKTAG};
use crate::utils::rel::LockRelId;

// XactLockTableWait operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XLTW_Oper {
    XltwNone,
    XltwUpdate,
    XltwDelete,
    XltwLock,
    XltwLockUpdated,
    XltwInsertIndex,
    XltwInsertIndexUnique,
    XltwFetchUpdated,
    XltwRecheckExclusionConstr,
}

#[deprecated(note = "TODO(struct-forward): repoint to crate::utils::rel::Relation in Phase 2")]
pub struct Relation; // TODO(struct-forward)
#[deprecated(note = "TODO(struct-forward): repoint to crate::c::TransactionId in Phase 2")]
pub type TransactionId = u32; // TODO(struct-forward)

#[allow(deprecated)]
pub fn RelationInitLockInfo(_relation: &Relation) {
    unimplemented!()
}

// Lock a relation. Conditional variants return whether the lock was acquired.
pub fn LockRelationOid(_relid: Oid, _lockmode: LOCKMODE) {
    unimplemented!()
}

pub fn LockRelationId(_relid: &LockRelId, _lockmode: LOCKMODE) {
    unimplemented!()
}

pub fn ConditionalLockRelationOid(_relid: Oid, _lockmode: LOCKMODE) -> bool {
    unimplemented!()
}

pub fn UnlockRelationId(_relid: &LockRelId, _lockmode: LOCKMODE) {
    unimplemented!()
}

pub fn UnlockRelationOid(_relid: Oid, _lockmode: LOCKMODE) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn LockRelation(_relation: &Relation, _lockmode: LOCKMODE) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn ConditionalLockRelation(_relation: &Relation, _lockmode: LOCKMODE) -> bool {
    unimplemented!()
}

#[allow(deprecated)]
pub fn UnlockRelation(_relation: &Relation, _lockmode: LOCKMODE) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn CheckRelationLockedByMe(_relation: &Relation, _lockmode: LOCKMODE, _orstronger: bool) -> bool {
    unimplemented!()
}

pub fn CheckRelationOidLockedByMe(_relid: Oid, _lockmode: LOCKMODE, _orstronger: bool) -> bool {
    unimplemented!()
}

#[allow(deprecated)]
pub fn LockHasWaitersRelation(_relation: &Relation, _lockmode: LOCKMODE) -> bool {
    unimplemented!()
}

pub fn LockRelationIdForSession(_relid: &LockRelId, _lockmode: LOCKMODE) {
    unimplemented!()
}

pub fn UnlockRelationIdForSession(_relid: &LockRelId, _lockmode: LOCKMODE) {
    unimplemented!()
}

// Lock a relation for extension.
#[allow(deprecated)]
pub fn LockRelationForExtension(_relation: &Relation, _lockmode: LOCKMODE) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn UnlockRelationForExtension(_relation: &Relation, _lockmode: LOCKMODE) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn ConditionalLockRelationForExtension(_relation: &Relation, _lockmode: LOCKMODE) -> bool {
    unimplemented!()
}

#[allow(deprecated)]
pub fn RelationExtensionLockWaiterCount(_relation: &Relation) -> i32 {
    unimplemented!()
}

// Lock to recompute pg_database.datfrozenxid in the current database.
pub fn LockDatabaseFrozenIds(_lockmode: LOCKMODE) {
    unimplemented!()
}

// Lock a page (currently only used within indexes).
#[allow(deprecated)]
pub fn LockPage(_relation: &Relation, _blkno: BlockNumber, _lockmode: LOCKMODE) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn ConditionalLockPage(_relation: &Relation, _blkno: BlockNumber, _lockmode: LOCKMODE) -> bool {
    unimplemented!()
}

#[allow(deprecated)]
pub fn UnlockPage(_relation: &Relation, _blkno: BlockNumber, _lockmode: LOCKMODE) {
    unimplemented!()
}

// Lock a tuple (see heap_lock_tuple before assuming you understand this).
#[allow(deprecated)]
pub fn LockTuple(_relation: &Relation, _tid: &ItemPointerData, _lockmode: LOCKMODE) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn ConditionalLockTuple(
    _relation: &Relation,
    _tid: &ItemPointerData,
    _lockmode: LOCKMODE,
    _log_lock_failure: bool,
) -> bool {
    unimplemented!()
}

#[allow(deprecated)]
pub fn UnlockTuple(_relation: &Relation, _tid: &ItemPointerData, _lockmode: LOCKMODE) {
    unimplemented!()
}

// Lock an XID (used to wait for a transaction to finish).
#[allow(deprecated)]
pub fn XactLockTableInsert(_xid: TransactionId) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn XactLockTableDelete(_xid: TransactionId) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn XactLockTableWait(
    _xid: TransactionId,
    _rel: &Relation,
    _ctid: &ItemPointerData,
    _oper: XLTW_Oper,
) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn ConditionalXactLockTableWait(_xid: TransactionId, _log_lock_failure: bool) -> bool {
    unimplemented!()
}

// Lock VXIDs, specified by conflicting locktags.
pub fn WaitForLockers(_heaplocktag: LOCKTAG, _lockmode: LOCKMODE, _progress: bool) {
    unimplemented!()
}

pub fn WaitForLockersMultiple(_locktags: &[LOCKTAG], _lockmode: LOCKMODE, _progress: bool) {
    unimplemented!()
}

// Lock an XID for tuple insertion (wait for an insertion to finish).
#[allow(deprecated)]
pub fn SpeculativeInsertionLockAcquire(_xid: TransactionId) -> u32 {
    unimplemented!()
}

#[allow(deprecated)]
pub fn SpeculativeInsertionLockRelease(_xid: TransactionId) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn SpeculativeInsertionWait(_xid: TransactionId, _token: u32) {
    unimplemented!()
}

// Lock a general object (other than a relation) of the current database.
pub fn LockDatabaseObject(_classid: Oid, _objid: Oid, _objsubid: u16, _lockmode: LOCKMODE) {
    unimplemented!()
}

pub fn ConditionalLockDatabaseObject(
    _classid: Oid,
    _objid: Oid,
    _objsubid: u16,
    _lockmode: LOCKMODE,
) -> bool {
    unimplemented!()
}

pub fn UnlockDatabaseObject(_classid: Oid, _objid: Oid, _objsubid: u16, _lockmode: LOCKMODE) {
    unimplemented!()
}

// Lock a shared-across-databases object (other than a relation).
pub fn LockSharedObject(_classid: Oid, _objid: Oid, _objsubid: u16, _lockmode: LOCKMODE) {
    unimplemented!()
}

pub fn ConditionalLockSharedObject(
    _classid: Oid,
    _objid: Oid,
    _objsubid: u16,
    _lockmode: LOCKMODE,
) -> bool {
    unimplemented!()
}

pub fn UnlockSharedObject(_classid: Oid, _objid: Oid, _objsubid: u16, _lockmode: LOCKMODE) {
    unimplemented!()
}

pub fn LockSharedObjectForSession(_classid: Oid, _objid: Oid, _objsubid: u16, _lockmode: LOCKMODE) {
    unimplemented!()
}

pub fn UnlockSharedObjectForSession(
    _classid: Oid,
    _objid: Oid,
    _objsubid: u16,
    _lockmode: LOCKMODE,
) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn LockApplyTransactionForSession(
    _suboid: Oid,
    _xid: TransactionId,
    _objid: u16,
    _lockmode: LOCKMODE,
) {
    unimplemented!()
}

#[allow(deprecated)]
pub fn UnlockApplyTransactionForSession(
    _suboid: Oid,
    _xid: TransactionId,
    _objid: u16,
    _lockmode: LOCKMODE,
) {
    unimplemented!()
}

// Describe a locktag for error messages; StringInfo -> &mut String.
pub fn DescribeLockTag(_buf: &mut String, _tag: &LOCKTAG) {
    unimplemented!()
}

pub fn GetLockNameFromTagType(_locktag_type: u16) -> &'static str {
    unimplemented!()
}
