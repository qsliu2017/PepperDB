//! Translated from PostgreSQL src/include/nodes/lockoptions.h

/// Strengths of FOR UPDATE/SHARE clauses. Order matters: highest value wins.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LockClauseStrength {
    NONE,           // no such clause - only used in PlanRowMark
    FORKEYSHARE,    // FOR KEY SHARE
    FORSHARE,       // FOR SHARE
    FORNOKEYUPDATE, // FOR NO KEY UPDATE
    FORUPDATE,      // FOR UPDATE
}

/// How to deal with rows being locked (NOWAIT / SKIP LOCKED). Order matters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LockWaitPolicy {
    LockWaitBlock, // wait for the lock (default)
    LockWaitSkip,  // skip rows that can't be locked (SKIP LOCKED)
    LockWaitError, // raise an error if a row cannot be locked (NOWAIT)
}

/// Possible lock modes for a tuple.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LockTupleMode {
    LockTupleKeyShare,       // SELECT FOR KEY SHARE
    LockTupleShare,          // SELECT FOR SHARE
    LockTupleNoKeyExclusive, // FOR NO KEY UPDATE, non-key UPDATEs
    LockTupleExclusive,      // FOR UPDATE, key UPDATEs, DELETE
}
