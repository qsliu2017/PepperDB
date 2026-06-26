//! Translated from PostgreSQL src/include/storage/lmgr.h
//!
//! High-level lock manager API: LOCKTAG-based wrappers over the heavyweight lock
//! manager (`crate::storage::lock`). The wrapper bodies live in
//! `crate::backend::storage::lmgr::lmgr` (lmgr.c, 15d); this header keeps the
//! header-origin `XLTW_Oper` enum and re-exports the C-named functions (rules s2:
//! the non-type-centric global-state functions rewire to `pub use`, no shim).

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

// --- function prototypes: rewired to the backend module (lmgr.c bodies, 15d) ---

pub use crate::backend::storage::lmgr::lmgr::{
    CheckRelationLockedByMe, CheckRelationOidLockedByMe, ConditionalLockDatabaseObject,
    ConditionalLockPage, ConditionalLockRelation, ConditionalLockRelationForExtension,
    ConditionalLockRelationOid, ConditionalLockSharedObject, ConditionalLockTuple,
    ConditionalXactLockTableWait, DescribeLockTag, GetLockNameFromTagType, LockApplyTransactionForSession,
    LockDatabaseFrozenIds, LockDatabaseObject, LockHasWaitersRelation, LockPage, LockRelation,
    LockRelationForExtension, LockRelationId, LockRelationIdForSession, LockRelationOid,
    LockSharedObject, LockSharedObjectForSession, LockTuple, RelationExtensionLockWaiterCount,
    RelationInitLockInfo, SpeculativeInsertionLockAcquire, SpeculativeInsertionLockRelease,
    SpeculativeInsertionWait, UnlockApplyTransactionForSession, UnlockDatabaseObject, UnlockPage,
    UnlockRelation, UnlockRelationForExtension, UnlockRelationId, UnlockRelationIdForSession,
    UnlockRelationOid, UnlockSharedObject, UnlockSharedObjectForSession, UnlockTuple,
    WaitForLockers, WaitForLockersMultiple, XactLockTableDelete, XactLockTableInsert,
    XactLockTableWait, speculative_token_scope,
};
