//! Translated from PostgreSQL src/include/storage/proclist.h
//!
//! Operations on doubly-linked lists of pgprocnos. Like dlist from ilist.h but
//! uses ProcNumber instead of pointers, so a list can be mapped at different
//! addresses in different backends.
//!
//! In C each link `proclist_node` lives inside the PGPROC at a fixed
//! `node_offset`, reached via `GetPGProcByNumber(procno)`. Under the
//! single-process port the link nodes are passed in as an index-keyed slice
//! `nodes: &mut [proclist_node]` (one entry per ProcNumber for the chosen link
//! field); the `node_offset` argument disappears.

use crate::storage::procnumber::{ProcNumber, INVALID_PROC_NUMBER};
use crate::storage::proclist_types::{proclist_head, proclist_mutable_iter, proclist_node};

/// Initialize a proclist.
pub fn proclist_init(list: &mut proclist_head) {
    list.head = INVALID_PROC_NUMBER;
    list.tail = INVALID_PROC_NUMBER;
}

/// Is the list empty?
pub fn proclist_is_empty(list: &proclist_head) -> bool {
    list.head == INVALID_PROC_NUMBER
}

/// Get the link node for `procno`. Replaces `proclist_node_get(procno,
/// node_offset)`: index the caller-supplied node slice instead of pointer math
/// inside PGPROC.
pub fn proclist_node_get(nodes: &mut [proclist_node], procno: ProcNumber) -> &mut proclist_node {
    &mut nodes[procno as usize]
}

/// Insert a process at the beginning of a list.
pub fn proclist_push_head(
    list: &mut proclist_head,
    procno: ProcNumber,
    nodes: &mut [proclist_node],
) {
    debug_assert!(nodes[procno as usize].next == 0 && nodes[procno as usize].prev == 0);

    if list.head == INVALID_PROC_NUMBER {
        debug_assert_eq!(list.tail, INVALID_PROC_NUMBER);
        nodes[procno as usize].next = INVALID_PROC_NUMBER;
        nodes[procno as usize].prev = INVALID_PROC_NUMBER;
        list.head = procno;
        list.tail = procno;
    } else {
        debug_assert!(list.tail != INVALID_PROC_NUMBER);
        debug_assert_ne!(list.head, procno);
        debug_assert_ne!(list.tail, procno);
        let old_head = list.head;
        nodes[procno as usize].next = old_head;
        nodes[old_head as usize].prev = procno;
        nodes[procno as usize].prev = INVALID_PROC_NUMBER;
        list.head = procno;
    }
}

/// Insert a process at the end of a list.
pub fn proclist_push_tail(
    list: &mut proclist_head,
    procno: ProcNumber,
    nodes: &mut [proclist_node],
) {
    debug_assert!(nodes[procno as usize].next == 0 && nodes[procno as usize].prev == 0);

    if list.tail == INVALID_PROC_NUMBER {
        debug_assert_eq!(list.head, INVALID_PROC_NUMBER);
        nodes[procno as usize].next = INVALID_PROC_NUMBER;
        nodes[procno as usize].prev = INVALID_PROC_NUMBER;
        list.head = procno;
    } else {
        debug_assert!(list.head != INVALID_PROC_NUMBER);
        debug_assert_ne!(list.head, procno);
        debug_assert_ne!(list.tail, procno);
        let old_tail = list.tail;
        nodes[procno as usize].prev = old_tail;
        nodes[old_tail as usize].next = procno;
        nodes[procno as usize].next = INVALID_PROC_NUMBER;
    }
    list.tail = procno;
}

/// Delete a process from a list --- it must be in the list!
pub fn proclist_delete(list: &mut proclist_head, procno: ProcNumber, nodes: &mut [proclist_node]) {
    let node = nodes[procno as usize];
    debug_assert!(node.next != 0 || node.prev != 0);

    if node.prev == INVALID_PROC_NUMBER {
        debug_assert_eq!(list.head, procno);
        list.head = node.next;
    } else {
        nodes[node.prev as usize].next = node.next;
    }

    if node.next == INVALID_PROC_NUMBER {
        debug_assert_eq!(list.tail, procno);
        list.tail = node.prev;
    } else {
        nodes[node.next as usize].prev = node.prev;
    }

    nodes[procno as usize].next = 0;
    nodes[procno as usize].prev = 0;
}

/// Check if a process is currently in a list. The caller must know the process
/// is in no _other_ list sharing the same link node.
pub fn proclist_contains(list: &proclist_head, procno: ProcNumber, nodes: &[proclist_node]) -> bool {
    let node = &nodes[procno as usize];

    if node.prev == 0 && node.next == 0 {
        return false;
    }

    debug_assert!(node.prev != INVALID_PROC_NUMBER || list.head == procno);
    debug_assert!(node.next != INVALID_PROC_NUMBER || list.tail == procno);

    true
}

/// Remove and return the first ProcNumber from a list (there must be one). C
/// returns the `PGPROC *`; here we hand back the head ProcNumber (resolve to a
/// PGPROC at the call site).
pub fn proclist_pop_head_node(list: &mut proclist_head, nodes: &mut [proclist_node]) -> ProcNumber {
    debug_assert!(!proclist_is_empty(list));
    let head = list.head;
    proclist_delete(list, head, nodes);
    head
}

/// Iterator over a proclist allowing deletion of the current node while
/// iterating. Mirrors `proclist_foreach_modify`: stash `next` before yielding
/// `cur` so the caller may `proclist_delete(list, iter.cur, nodes)`.
pub fn proclist_iter_init(list: &proclist_head, nodes: &[proclist_node]) -> proclist_mutable_iter {
    let cur = list.head;
    let next = if cur == INVALID_PROC_NUMBER {
        INVALID_PROC_NUMBER
    } else {
        nodes[cur as usize].next
    };
    proclist_mutable_iter { cur, next }
}

/// Advance a `proclist_foreach_modify`-style iterator; returns the previous
/// `cur`, or `None` when exhausted.
pub fn proclist_iter_next(iter: &mut proclist_mutable_iter, nodes: &[proclist_node]) -> Option<ProcNumber> {
    if iter.cur == INVALID_PROC_NUMBER {
        return None;
    }
    let cur = iter.cur;
    iter.cur = iter.next;
    iter.next = if iter.cur == INVALID_PROC_NUMBER {
        INVALID_PROC_NUMBER
    } else {
        nodes[iter.cur as usize].next
    };
    Some(cur)
}
