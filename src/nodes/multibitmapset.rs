//! Lists of Bitmapsets.
//!
//! Source: postgres/src/backend/nodes/multibitmapset.c
//! Merged header: postgres/src/include/nodes/multibitmapset.h
//!
//! A multibitmapset is useful in situations where members of a set can
//! be identified by two small integers; for example, varno and varattno
//! of a group of Vars within a query.  The implementation is a List of
//! Bitmapsets, so that the empty set can be represented by NIL.  (But,
//! as with Bitmapsets, that's not the only allowed representation.)
//! The zero-based index of a List element is the first identifying value,
//! and the (also zero-based) index of a bit within that Bitmapset is
//! the second identifying value.  There is no expectation that the
//! Bitmapsets should all be the same size.
//!
//! The available operations on multibitmapsets are intended to parallel
//! those on bitmapsets, for example union and intersection.  So far only
//! a small fraction of that has been built out; we'll add more as needed.
//!
//! FULLY REAL over the ported Bitmapset + List.

use crate::prelude::*;

use crate::nodes::bitmapset::{
    bms_add_member, bms_add_members, bms_int_members, bms_is_member, bms_overlap, Bitmapset,
};
use crate::nodes::pg_list::{
    lappend, lfirst_mut, list_cell_number, list_length, list_nth_cell, list_truncate, List,
    ListCell,
};
use crate::{forboth, lfirst_node, list_nth_node};

/// mbms_add_member
///     Add a new member to a multibitmapset.
///
/// The new member is identified by "listidx", the zero-based index of the
/// List element it should go into, and "bitidx", which specifies the bit
/// number to be set therein.
///
/// This is like bms_add_member, but for multibitmapsets.
///
/// # Safety
/// `a` must be NIL or a valid pointer List of Bitmapset pointers.
pub unsafe fn mbms_add_member(mut a: *mut List, listidx: c_int, bitidx: c_int) -> *mut List {
    let bms: *mut Bitmapset;
    let lc: *mut ListCell;

    if listidx < 0 || bitidx < 0 {
        elog!(ERROR, "negative multibitmapset member index not allowed");
    }
    // Add empty elements as needed
    while list_length(a) <= listidx {
        a = lappend(a, null_mut());
    }
    // Update the target element
    lc = list_nth_cell(a, listidx);
    bms = lfirst_node!(Bitmapset, T_Bitmapset, lc);
    let bms = bms_add_member(bms, bitidx);
    *lfirst_mut(lc) = bms as *mut c_void;
    a
}

/// mbms_add_members
///     Add all members of set b to set a.
///
/// This is a UNION operation, but the left input is modified in-place.
///
/// This is like bms_add_members, but for multibitmapsets.
///
/// # Safety
/// `a` and `b` must be NIL or valid pointer Lists of Bitmapset pointers.
#[no_mangle]
pub unsafe fn mbms_add_members(mut a: *mut List, b: *const List) -> *mut List {
    // Add empty elements to a, as needed
    while list_length(a) < list_length(b) {
        a = lappend(a, null_mut());
    }
    // forboth will stop at the end of the shorter list, which is fine
    forboth!(lca, a, lcb, b, {
        let bmsa = lfirst_node!(Bitmapset, T_Bitmapset, lca);
        let bmsb = lfirst_node!(Bitmapset, T_Bitmapset, lcb) as *const Bitmapset;

        let bmsa = bms_add_members(bmsa, bmsb);
        *lfirst_mut(lca) = bmsa as *mut c_void;
    });
    a
}

/// mbms_int_members
///     Reduce set a to its intersection with set b.
///
/// This is an INTERSECT operation, but the left input is modified in-place.
///
/// This is like bms_int_members, but for multibitmapsets.
///
/// # Safety
/// `a` and `b` must be NIL or valid pointer Lists of Bitmapset pointers.
pub unsafe fn mbms_int_members(mut a: *mut List, b: *const List) -> *mut List {
    // Remove any elements of a that are no longer of use
    a = list_truncate(a, list_length(b));
    // forboth will stop at the end of the shorter list, which is fine
    forboth!(lca, a, lcb, b, {
        let bmsa = lfirst_node!(Bitmapset, T_Bitmapset, lca);
        let bmsb = lfirst_node!(Bitmapset, T_Bitmapset, lcb) as *const Bitmapset;

        let bmsa = bms_int_members(bmsa, bmsb);
        *lfirst_mut(lca) = bmsa as *mut c_void;
    });
    a
}

/// mbms_is_member
///     Is listidx/bitidx a member of A?
///
/// This is like bms_is_member, but for multibitmapsets.
///
/// # Safety
/// `a` must be NIL or a valid pointer List of Bitmapset pointers.
pub unsafe fn mbms_is_member(listidx: c_int, bitidx: c_int, a: *const List) -> bool {
    let bms: *const Bitmapset;

    // XXX better to just return false for negative indexes?
    if listidx < 0 || bitidx < 0 {
        elog!(ERROR, "negative multibitmapset member index not allowed");
    }
    if listidx >= list_length(a) {
        return false;
    }
    bms = list_nth_node!(Bitmapset, T_Bitmapset, a, listidx) as *const Bitmapset;
    bms_is_member(bitidx, bms)
}

/// mbms_overlap_sets
///     Identify the bitmapsets having common members in a and b.
///
/// The result is a bitmapset of the list indexes of bitmapsets that overlap.
///
/// # Safety
/// `a` and `b` must be NIL or valid pointer Lists of Bitmapset pointers.
#[no_mangle]
pub unsafe fn mbms_overlap_sets(a: *const List, b: *const List) -> *mut Bitmapset {
    let mut result: *mut Bitmapset = null_mut();

    // forboth will stop at the end of the shorter list, which is fine
    forboth!(lca, a, lcb, b, {
        let bmsa = lfirst_node!(Bitmapset, T_Bitmapset, lca) as *const Bitmapset;
        let bmsb = lfirst_node!(Bitmapset, T_Bitmapset, lcb) as *const Bitmapset;

        if bms_overlap(bmsa, bmsb) {
            // C uses foreach_current_index(lca); since the Rust `forboth`
            // binds `lca` as a real cell pointer within list `a`, the
            // zero-based index is recovered via list_cell_number().
            result = bms_add_member(result, list_cell_number(a, lca));
        }
    });
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::bitmapset::bms_is_empty;
    use crate::nodes::pg_list::NIL;

    #[test]
    fn test_add_member_and_is_member() {
        unsafe {
            let mut a: *mut List = NIL;
            // Set (listidx=0,bit=3), (listidx=2,bit=1)
            a = mbms_add_member(a, 0, 3);
            a = mbms_add_member(a, 2, 1);

            // List grew to length 3 (indices 0,1,2)
            assert_eq!(list_length(a), 3);

            assert!(mbms_is_member(0, 3, a));
            assert!(mbms_is_member(2, 1, a));
            assert!(!mbms_is_member(0, 1, a));
            assert!(!mbms_is_member(1, 0, a)); // middle element is empty
            assert!(!mbms_is_member(5, 0, a)); // out of range
        }
    }

    #[test]
    fn test_add_members_union() {
        unsafe {
            let mut a: *mut List = NIL;
            a = mbms_add_member(a, 0, 1);
            a = mbms_add_member(a, 1, 2);

            let mut b: *mut List = NIL;
            b = mbms_add_member(b, 0, 5);
            b = mbms_add_member(b, 2, 7);

            a = mbms_add_members(a, b);

            // a now has length 3 (extended to match b)
            assert_eq!(list_length(a), 3);
            assert!(mbms_is_member(0, 1, a));
            assert!(mbms_is_member(0, 5, a));
            assert!(mbms_is_member(1, 2, a));
            assert!(mbms_is_member(2, 7, a));
        }
    }

    #[test]
    fn test_int_members_intersect() {
        unsafe {
            let mut a: *mut List = NIL;
            a = mbms_add_member(a, 0, 1);
            a = mbms_add_member(a, 0, 4);
            a = mbms_add_member(a, 1, 2);
            a = mbms_add_member(a, 2, 9);

            let mut b: *mut List = NIL;
            b = mbms_add_member(b, 0, 4);
            b = mbms_add_member(b, 1, 8);

            a = mbms_int_members(a, b);

            // Truncated to length of b (2)
            assert_eq!(list_length(a), 2);
            // Index 0: {1,4} INT {4} = {4}
            assert!(mbms_is_member(0, 4, a));
            assert!(!mbms_is_member(0, 1, a));
            // Index 1: {2} INT {8} = {} (empty)
            assert!(!mbms_is_member(1, 2, a));
            assert!(!mbms_is_member(1, 8, a));
        }
    }

    #[test]
    fn test_overlap_sets() {
        unsafe {
            let mut a: *mut List = NIL;
            a = mbms_add_member(a, 0, 1);
            a = mbms_add_member(a, 1, 2);
            a = mbms_add_member(a, 2, 3);

            let mut b: *mut List = NIL;
            b = mbms_add_member(b, 0, 1); // overlaps at index 0
            b = mbms_add_member(b, 1, 9); // no overlap at index 1
            b = mbms_add_member(b, 2, 3); // overlaps at index 2

            let overlap = mbms_overlap_sets(a, b);
            assert!(!bms_is_empty(overlap));
            assert!(bms_is_member(0, overlap));
            assert!(!bms_is_member(1, overlap));
            assert!(bms_is_member(2, overlap));
        }
    }

    #[test]
    fn test_empty_inputs() {
        unsafe {
            // overlap of two empty multibitmapsets is empty
            let r = mbms_overlap_sets(NIL, NIL);
            assert!(bms_is_empty(r));
            // is_member on empty list
            assert!(!mbms_is_member(0, 0, NIL));
        }
    }
}
