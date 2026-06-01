//! Translation of postgres/src/include/nodes/lockoptions.h
//!
//! Common locking-related declarations (FOR UPDATE/SHARE strengths, wait policy,
//! tuple lock modes).
//!
//! Copyright (c) 2014-2025, PostgreSQL Global Development Group

/// Strengths of FOR UPDATE/SHARE clauses. The ordering matters: the highest
/// numerical value takes precedence when an RTE is specified multiple ways
/// (see applyLockingClause).
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum LockClauseStrength {
    /// no such clause - only used in PlanRowMark
    LCS_NONE,
    /// FOR KEY SHARE
    LCS_FORKEYSHARE,
    /// FOR SHARE
    LCS_FORSHARE,
    /// FOR NO KEY UPDATE
    LCS_FORNOKEYUPDATE,
    /// FOR UPDATE
    LCS_FORUPDATE,
}
pub use LockClauseStrength::*;

/// How to deal with rows being locked by FOR UPDATE/SHARE (NOWAIT and SKIP
/// LOCKED options). Ordering matters: highest value takes precedence.
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum LockWaitPolicy {
    /// Wait for the lock to become available (default behavior)
    LockWaitBlock,
    /// Skip rows that can't be locked (SKIP LOCKED)
    LockWaitSkip,
    /// Raise an error if a row cannot be locked (NOWAIT)
    LockWaitError,
}
pub use LockWaitPolicy::*;

/// Possible lock modes for a tuple.
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum LockTupleMode {
    /// SELECT FOR KEY SHARE
    LockTupleKeyShare,
    /// SELECT FOR SHARE
    LockTupleShare,
    /// SELECT FOR NO KEY UPDATE, and UPDATEs that don't modify key columns
    LockTupleNoKeyExclusive,
    /// SELECT FOR UPDATE, UPDATEs that modify key columns, and DELETE
    LockTupleExclusive,
}
pub use LockTupleMode::*;
