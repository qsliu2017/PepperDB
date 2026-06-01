//! storage/proclist_types.h - doubly-linked lists of pgprocnos

use std::ffi::c_int;

// ProcNumber's canonical home is storage/procnumber.h (not yet ported).
// Mirror its definition (a c_int pgprocno) locally.
// TODO: dedup when storage/procnumber.h lands.
pub type ProcNumber = c_int;

/*
 * A node in a doubly-linked list of processes.  The link fields contain
 * the 0-based PGPROC indexes of the next and previous process, or
 * INVALID_PROC_NUMBER in the next-link of the last node and the prev-link
 * of the first node.  A node that is currently not in any list
 * should have next == prev == 0; this is not a possible state for a node
 * that is in a list, because we disallow circularity.
 */
#[repr(C)]
pub struct proclist_node {
    pub next: ProcNumber, /* pgprocno of the next PGPROC */
    pub prev: ProcNumber, /* pgprocno of the prev PGPROC */
}

/*
 * Header of a doubly-linked list of PGPROCs, identified by pgprocno.
 * An empty list is represented by head == tail == INVALID_PROC_NUMBER.
 */
#[repr(C)]
pub struct proclist_head {
    pub head: ProcNumber, /* pgprocno of the head PGPROC */
    pub tail: ProcNumber, /* pgprocno of the tail PGPROC */
}

/*
 * List iterator allowing some modifications while iterating.
 */
#[repr(C)]
pub struct proclist_mutable_iter {
    pub cur: ProcNumber,  /* pgprocno of the current PGPROC */
    pub next: ProcNumber, /* pgprocno of the next PGPROC */
}
