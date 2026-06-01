//! storage/proclist.h - operations on doubly-linked lists of pgprocnos

use std::ffi::c_int;

use crate::c::Size;
use crate::storage::procnumber::INVALID_PROC_NUMBER;
use crate::storage::proclist_types::{proclist_head, proclist_mutable_iter, proclist_node};

// PGPROC and GetPGProcByNumber are declared in storage/proc.h, which is not
// yet ported. Provide minimal local stubs so this header translates faithfully.
// TODO: dedup with storage/proc.rs once it lands.
#[repr(C)]
pub struct PGPROC {
    _private: [u8; 0],
}

/// GetPGProcByNumber(n) - return pointer to the PGPROC with the given procno.
/// Provided by storage/proc.h in upstream; stubbed here.
// TODO: dedup with storage/proc.rs once it lands.
#[inline]
pub unsafe fn GetPGProcByNumber(_n: c_int) -> *mut PGPROC {
    unimplemented!()
}

/*
 * Initialize a proclist.
 */
#[inline]
pub unsafe fn proclist_init(list: *mut proclist_head) {
    (*list).head = INVALID_PROC_NUMBER;
    (*list).tail = INVALID_PROC_NUMBER;
}

/*
 * Is the list empty?
 */
#[inline]
pub unsafe fn proclist_is_empty(list: *const proclist_head) -> bool {
    (*list).head == INVALID_PROC_NUMBER
}

/*
 * Get a pointer to a proclist_node inside a given PGPROC, given a procno and
 * the proclist_node field's offset within struct PGPROC.
 */
#[inline]
pub unsafe fn proclist_node_get(procno: c_int, node_offset: Size) -> *mut proclist_node {
    let entry = GetPGProcByNumber(procno) as *mut u8;
    entry.add(node_offset) as *mut proclist_node
}

/*
 * Insert a process at the beginning of a list.
 */
#[inline]
pub unsafe fn proclist_push_head_offset(
    list: *mut proclist_head,
    procno: c_int,
    node_offset: Size,
) {
    let node = proclist_node_get(procno, node_offset);

    assert!((*node).next == 0 && (*node).prev == 0);

    if (*list).head == INVALID_PROC_NUMBER {
        assert!((*list).tail == INVALID_PROC_NUMBER);
        (*node).next = INVALID_PROC_NUMBER;
        (*node).prev = INVALID_PROC_NUMBER;
        (*list).head = procno;
        (*list).tail = procno;
    } else {
        assert!((*list).tail != INVALID_PROC_NUMBER);
        assert!((*list).head != procno);
        assert!((*list).tail != procno);
        (*node).next = (*list).head;
        (*proclist_node_get((*node).next, node_offset)).prev = procno;
        (*node).prev = INVALID_PROC_NUMBER;
        (*list).head = procno;
    }
}

/*
 * Insert a process at the end of a list.
 */
#[inline]
pub unsafe fn proclist_push_tail_offset(
    list: *mut proclist_head,
    procno: c_int,
    node_offset: Size,
) {
    let node = proclist_node_get(procno, node_offset);

    assert!((*node).next == 0 && (*node).prev == 0);

    if (*list).tail == INVALID_PROC_NUMBER {
        assert!((*list).head == INVALID_PROC_NUMBER);
        (*node).next = INVALID_PROC_NUMBER;
        (*node).prev = INVALID_PROC_NUMBER;
        (*list).head = procno;
        (*list).tail = procno;
    } else {
        assert!((*list).head != INVALID_PROC_NUMBER);
        assert!((*list).head != procno);
        assert!((*list).tail != procno);
        (*node).prev = (*list).tail;
        (*proclist_node_get((*node).prev, node_offset)).next = procno;
        (*node).next = INVALID_PROC_NUMBER;
        (*list).tail = procno;
    }
}

/*
 * Delete a process from a list --- it must be in the list!
 */
#[inline]
pub unsafe fn proclist_delete_offset(list: *mut proclist_head, procno: c_int, node_offset: Size) {
    let node = proclist_node_get(procno, node_offset);

    assert!((*node).next != 0 || (*node).prev != 0);

    if (*node).prev == INVALID_PROC_NUMBER {
        assert!((*list).head == procno);
        (*list).head = (*node).next;
    } else {
        (*proclist_node_get((*node).prev, node_offset)).next = (*node).next;
    }

    if (*node).next == INVALID_PROC_NUMBER {
        assert!((*list).tail == procno);
        (*list).tail = (*node).prev;
    } else {
        (*proclist_node_get((*node).next, node_offset)).prev = (*node).prev;
    }

    (*node).next = 0;
    (*node).prev = 0;
}

/*
 * Check if a process is currently in a list.  It must be known that the
 * process is not in any _other_ proclist that uses the same proclist_node,
 * so that the only possibilities are that it is in this list or none.
 */
#[inline]
pub unsafe fn proclist_contains_offset(
    list: *const proclist_head,
    procno: c_int,
    node_offset: Size,
) -> bool {
    let node = proclist_node_get(procno, node_offset) as *const proclist_node;

    /* If it's not in any list, it's definitely not in this one. */
    if (*node).prev == 0 && (*node).next == 0 {
        return false;
    }

    /*
     * It must, in fact, be in this list.  Ideally, in assert-enabled builds,
     * we'd verify that.  But since this function is typically used while
     * holding a spinlock, crawling the whole list is unacceptable.  However,
     * we can verify matters in O(1) time when the node is a list head or
     * tail, and that seems worth doing, since in practice that should often
     * be enough to catch mistakes.
     */
    assert!((*node).prev != INVALID_PROC_NUMBER || (*list).head == procno);
    assert!((*node).next != INVALID_PROC_NUMBER || (*list).tail == procno);

    true
}

/*
 * Remove and return the first process from a list (there must be one).
 */
#[inline]
pub unsafe fn proclist_pop_head_node_offset(
    list: *mut proclist_head,
    node_offset: Size,
) -> *mut PGPROC {
    assert!(!proclist_is_empty(list));
    let proc = GetPGProcByNumber((*list).head);
    proclist_delete_offset(list, (*list).head, node_offset);
    proc
}

/*
 * Helper macros to avoid repetition of offsetof(PGPROC, <member>).
 * 'link_member' is the name of a proclist_node member in PGPROC.
 */
#[macro_export]
macro_rules! proclist_delete {
    ($list:expr, $procno:expr, $link_member:ident) => {
        $crate::storage::proclist::proclist_delete_offset(
            $list,
            $procno,
            $crate::offset_of!($crate::storage::proc::PGPROC, $link_member),
        )
    };
}

#[macro_export]
macro_rules! proclist_push_head {
    ($list:expr, $procno:expr, $link_member:ident) => {
        $crate::storage::proclist::proclist_push_head_offset(
            $list,
            $procno,
            $crate::offset_of!($crate::storage::proc::PGPROC, $link_member),
        )
    };
}

#[macro_export]
macro_rules! proclist_push_tail {
    ($list:expr, $procno:expr, $link_member:ident) => {
        $crate::storage::proclist::proclist_push_tail_offset(
            $list,
            $procno,
            $crate::offset_of!($crate::storage::proc::PGPROC, $link_member),
        )
    };
}

#[macro_export]
macro_rules! proclist_pop_head_node {
    ($list:expr, $link_member:ident) => {
        $crate::storage::proclist::proclist_pop_head_node_offset(
            $list,
            $crate::offset_of!($crate::storage::proc::PGPROC, $link_member),
        )
    };
}

#[macro_export]
macro_rules! proclist_contains {
    ($list:expr, $procno:expr, $link_member:ident) => {
        $crate::storage::proclist::proclist_contains_offset(
            $list,
            $procno,
            $crate::offset_of!($crate::storage::proc::PGPROC, $link_member),
        )
    };
}

/*
 * Iterate through the list pointed at by 'lhead', storing the current
 * position in 'iter'.  'link_member' is the name of a proclist_node member in
 * PGPROC.  Access the current position with iter.cur.
 *
 * The only list modification allowed while iterating is deleting the current
 * node with proclist_delete(list, iter.cur, node_offset).
 *
 * The C version is a `for` loop with init/condition/post clauses. In Rust this
 * is rendered as a `while` loop; the caller wraps the body inside the macro
 * invocation. The AssertVariableIsOfTypeMacro type checks are dropped (Rust's
 * type system enforces them statically).
 */
#[macro_export]
macro_rules! proclist_foreach_modify {
    ($iter:expr, $lhead:expr, $link_member:ident, $body:block) => {{
        let __off = $crate::offset_of!($crate::storage::proc::PGPROC, $link_member);
        ($iter).cur = (*($lhead)).head;
        ($iter).next = if ($iter).cur == $crate::storage::procnumber::INVALID_PROC_NUMBER {
            $crate::storage::procnumber::INVALID_PROC_NUMBER
        } else {
            (*$crate::storage::proclist::proclist_node_get(($iter).cur, __off)).next
        };
        while ($iter).cur != $crate::storage::procnumber::INVALID_PROC_NUMBER {
            $body
            ($iter).cur = ($iter).next;
            ($iter).next = if ($iter).cur == $crate::storage::procnumber::INVALID_PROC_NUMBER {
                $crate::storage::procnumber::INVALID_PROC_NUMBER
            } else {
                (*$crate::storage::proclist::proclist_node_get(($iter).cur, __off)).next
            };
        }
    }};
}
