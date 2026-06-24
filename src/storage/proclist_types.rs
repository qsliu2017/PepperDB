//! Translated from PostgreSQL src/include/storage/proclist_types.h

use crate::storage::procnumber::ProcNumber;

/// A node in a doubly-linked list of processes. Links are 0-based PGPROC indexes
/// (ProcNumber), or INVALID_PROC_NUMBER at the ends. A not-in-list node has
/// next == prev == 0.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct proclist_node {
    pub next: ProcNumber,
    pub prev: ProcNumber,
}

/// Header of a doubly-linked PGPROC list. Empty list: head == tail ==
/// INVALID_PROC_NUMBER.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct proclist_head {
    pub head: ProcNumber,
    pub tail: ProcNumber,
}

/// List iterator allowing some modifications while iterating.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct proclist_mutable_iter {
    pub cur: ProcNumber,
    pub next: ProcNumber,
}
