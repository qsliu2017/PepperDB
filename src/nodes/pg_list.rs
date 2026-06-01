//! Translation of postgres/src/include/nodes/pg_list.h
//!                + postgres/src/backend/nodes/list.c
//!
//! Interface and implementation for the PostgreSQL generic list package.
//!
//! Once upon a time, parts of Postgres were written in Lisp and used real
//! cons-cell lists for major data structures.  When that code was rewritten
//! in C, we initially had a faithful emulation of cons-cell lists, which
//! unsurprisingly was a performance bottleneck.  A couple of major rewrites
//! later, these data structures are actually simple expansible arrays;
//! but the "List" name and a lot of the notation survives.
//!
//! One important concession to the original implementation is that an empty
//! list is always represented by a null pointer (preferentially written NIL).
//! Non-empty lists have a header, which will not be relocated as long as the
//! list remains non-empty, and an expansible data array.
//!
//! We support four types of lists:
//!
//!  T_List: lists of pointers
//!      (in practice usually pointers to Nodes, but not always;
//!      declared as "void *" to minimize casting annoyances)
//!  T_IntList: lists of integers
//!  T_OidList: lists of Oids
//!  T_XidList: lists of TransactionIds
//!      (the XidList infrastructure is less complete than the other cases)
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! ---------------------------------------------------------------------------
//! Translation notes (deviations from the C source):
//!
//! * The C looping macros (foreach, foreach_delete_current, for_each_from,
//!   for_each_cell, forboth, for_both_cell, forthree, forfour, forfive, and the
//!   typed foreach_ptr/int/oid/xid/node variants) rely on the C comma-operator
//!   for-loop, which Rust lacks.  Each is translated to a `macro_rules!` macro
//!   that takes a TRAILING BLOCK holding the loop body, e.g.
//!       foreach!(lc, list, { /* ... use current_cell!(lc) ... */ });
//!   `break`/`continue` inside the block behave as normal Rust loop control.
//!
//!   TWO loop-variable flavors (forced by macro hygiene on stable Rust):
//!   - Single-list `foreach!`/`for_each_from!`/`for_each_cell!`: the loop variable
//!     is a `ForEachState` CURSOR (not a bare pointer), because C's `cell##__state`
//!     token-paste cannot be reproduced and macro hygiene blocks a hidden shared
//!     state binding.  Get the current `*mut ListCell` with `current_cell!(lc)`;
//!     `foreach_delete_current!(list, lc)` and `foreach_current_index!(lc)` mutate
//!     the same cursor, mirroring the C `cell##__state` trick.
//!   - Multi-list `forboth!`/`for_both_cell!`/`forthree!`/`forfour!`/`forfive!`:
//!     each loop variable is a bare `*mut ListCell` (NULL once that list is
//!     exhausted), used directly as in C (e.g. `lfirst(cell1)`).  The C
//!     foreach_delete_current/foreach_current_index helpers do not apply to these.
//!
//! * `equal()` (from equalfuncs.c) is called with `*const c_void` arguments.
//!
//! * `list_make1`..`list_make5` (and the _int/_oid/_xid variants) are macros that
//!   call the `list_makeN_impl` functions translated below.
//!
//! * Functions that dereference raw pointers are `pub unsafe fn`.

use crate::prelude::*;
use crate::IsA;
use crate::nodes::equalfuncs::equal;
use crate::nodes::nodes::NodeTag::{T_IntList, T_List, T_OidList, T_XidList};
use crate::nodes::nodes::{Node, NodeTag};
use crate::port::pg_bitutils::pg_nextpower2_32;
use crate::utils::palloc::{GetMemoryChunkContext, MemoryContextAlloc};
use core::ffi::{c_int, c_void};

// ===========================================================================
//                       pg_list.h: types and inline API
// ===========================================================================

/// `union ListCell` - the storage of a single list element.
///
/// Reading or writing a union field is `unsafe` in Rust; callers use the
/// `lfirst`/`lfirst_int`/... accessors (themselves `unsafe`).
#[repr(C)]
#[derive(Clone, Copy)]
pub union ListCell {
    pub ptr_value: *mut c_void,
    pub int_value: c_int,
    pub oid_value: Oid,
    pub xid_value: TransactionId,
}

/// `struct List`.
///
/// This is the PG13+ single-allocation layout: when the list is short enough,
/// the cell array (`elements`) points into `initial_elements`, which is
/// co-allocated with the header in one palloc chunk.  `initial_elements` is a
/// trailing flexible array (`[ListCell; FLEXIBLE_ARRAY_MEMBER]`, i.e. length 0);
/// `new_list`/`enlarge_list` implement the trick.
#[repr(C)]
pub struct List {
    /// T_List, T_IntList, T_OidList, or T_XidList
    pub r#type: NodeTag,
    /// number of elements currently present
    pub length: c_int,
    /// allocated length of elements[]
    pub max_length: c_int,
    /// re-allocatable array of cells
    pub elements: *mut ListCell,
    /// We may allocate some cells along with the List header.
    /// If elements == initial_elements, it's not a separate allocation.
    pub initial_elements: [ListCell; FLEXIBLE_ARRAY_MEMBER],
}

/// The *only* valid representation of an empty list is NIL; in other
/// words, a non-NIL list is guaranteed to have length >= 1.
pub const NIL: *mut List = core::ptr::null_mut();

// ---------------------------------------------------------------------------
// State structs for the various looping macros below.
// ---------------------------------------------------------------------------

/// State for `foreach` / `for_each_from` / `for_each_cell`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ForEachState {
    /// list we're looping through
    pub l: *const List,
    /// current element index
    pub i: c_int,
}

/// State for `forboth`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ForBothState {
    /// lists we're looping through
    pub l1: *const List,
    pub l2: *const List,
    /// common element index
    pub i: c_int,
}

/// State for `for_both_cell`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ForBothCellState {
    /// lists we're looping through
    pub l1: *const List,
    pub l2: *const List,
    /// current element indexes
    pub i1: c_int,
    pub i2: c_int,
}

/// State for `forthree`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ForThreeState {
    /// lists we're looping through
    pub l1: *const List,
    pub l2: *const List,
    pub l3: *const List,
    /// common element index
    pub i: c_int,
}

/// State for `forfour`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ForFourState {
    /// lists we're looping through
    pub l1: *const List,
    pub l2: *const List,
    pub l3: *const List,
    pub l4: *const List,
    /// common element index
    pub i: c_int,
}

/// State for `forfive`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ForFiveState {
    /// lists we're looping through
    pub l1: *const List,
    pub l2: *const List,
    pub l3: *const List,
    pub l4: *const List,
    pub l5: *const List,
    /// common element index
    pub i: c_int,
}

/*
 * These routines are small enough, and used often enough, to justify being
 * inline.
 */

/// Fetch address of list's first cell; NULL if empty list.
///
/// # Safety
/// `l` must be NIL or a valid List pointer.
#[inline]
pub unsafe fn list_head(l: *const List) -> *mut ListCell {
    if !l.is_null() {
        (*l).elements.add(0)
    } else {
        core::ptr::null_mut()
    }
}

/// Fetch address of list's last cell; NULL if empty list.
///
/// # Safety
/// `l` must be NIL or a valid List pointer.
#[inline]
pub unsafe fn list_tail(l: *const List) -> *mut ListCell {
    if !l.is_null() {
        (*l).elements.add(((*l).length - 1) as usize)
    } else {
        core::ptr::null_mut()
    }
}

/// Fetch address of list's second cell, if it has one, else NULL.
///
/// # Safety
/// `l` must be NIL or a valid List pointer.
#[inline]
pub unsafe fn list_second_cell(l: *const List) -> *mut ListCell {
    if !l.is_null() && (*l).length >= 2 {
        (*l).elements.add(1)
    } else {
        core::ptr::null_mut()
    }
}

/// Fetch list's length.
///
/// # Safety
/// `l` must be NIL or a valid List pointer.
#[inline]
pub unsafe fn list_length(l: *const List) -> c_int {
    if !l.is_null() {
        (*l).length
    } else {
        0
    }
}

/*
 * Macros to access the data values within List cells.
 *
 * Note that with the exception of the "xxx_node" macros, these are
 * lvalues and can be assigned to.
 *
 * NB: There is an unfortunate legacy from a previous incarnation of
 * the List API: the macro lfirst() was used to mean "the data in this
 * cons cell". To avoid changing every usage of lfirst(), that meaning
 * has been kept. As a result, lfirst() takes a ListCell and returns
 * the data it contains; to get the data in the first cell of a
 * List, use linitial(). Worse, lsecond() is more closely related to
 * linitial() than lfirst(): given a List, lsecond() returns the data
 * in the second list cell.
 *
 * These were lvalue macros in C; we translate them as accessor fns that
 * return the value, plus `*_ref`/`set_*` helpers where list.c assigns to them.
 */

/// `lfirst(lc)` - the pointer value stored in cell `lc`.
///
/// # Safety
/// `lc` must point to a valid ListCell holding a pointer value.
#[inline]
pub unsafe fn lfirst(lc: *const ListCell) -> *mut c_void {
    (*lc).ptr_value
}

/// `lfirst_int(lc)` - the int value stored in cell `lc`.
///
/// # Safety
/// `lc` must point to a valid ListCell holding an int value.
#[inline]
pub unsafe fn lfirst_int(lc: *const ListCell) -> c_int {
    (*lc).int_value
}

/// `lfirst_oid(lc)` - the Oid value stored in cell `lc`.
///
/// # Safety
/// `lc` must point to a valid ListCell holding an Oid value.
#[inline]
pub unsafe fn lfirst_oid(lc: *const ListCell) -> Oid {
    (*lc).oid_value
}

/// `lfirst_xid(lc)` - the TransactionId value stored in cell `lc`.
///
/// # Safety
/// `lc` must point to a valid ListCell holding an xid value.
#[inline]
pub unsafe fn lfirst_xid(lc: *const ListCell) -> TransactionId {
    (*lc).xid_value
}

/// Mutable reference to a cell's pointer value (lvalue form of `lfirst`).
///
/// # Safety
/// `lc` must point to a valid, writable ListCell.
#[inline]
pub unsafe fn lfirst_mut(lc: *mut ListCell) -> *mut *mut c_void {
    core::ptr::addr_of_mut!((*lc).ptr_value)
}

/// Mutable reference to a cell's int value.
///
/// # Safety
/// `lc` must point to a valid, writable ListCell.
#[inline]
pub unsafe fn lfirst_int_mut(lc: *mut ListCell) -> *mut c_int {
    core::ptr::addr_of_mut!((*lc).int_value)
}

/// Mutable reference to a cell's Oid value.
///
/// # Safety
/// `lc` must point to a valid, writable ListCell.
#[inline]
pub unsafe fn lfirst_oid_mut(lc: *mut ListCell) -> *mut Oid {
    core::ptr::addr_of_mut!((*lc).oid_value)
}

/// Mutable reference to a cell's xid value.
///
/// # Safety
/// `lc` must point to a valid, writable ListCell.
#[inline]
pub unsafe fn lfirst_xid_mut(lc: *mut ListCell) -> *mut TransactionId {
    core::ptr::addr_of_mut!((*lc).xid_value)
}

/// `lfirst_node(type, lc)` - `castNode(type, lfirst(lc))`.
///
/// Pass the `T_`-prefixed tag explicitly: `lfirst_node!(MyNode, T_MyNode, lc)`.
#[macro_export]
macro_rules! lfirst_node {
    ($ty:ty, $tag:ident, $lc:expr) => {
        $crate::castNode!($ty, $tag, $crate::nodes::pg_list::lfirst($lc))
    };
}

// ---- linitial / lsecond / lthird / lfourth (by-list accessors) ----

/// `linitial(l)` = `lfirst(list_nth_cell(l, 0))`.
///
/// # Safety
/// `l` must be a non-NIL pointer List with at least 1 element.
#[inline]
pub unsafe fn linitial(l: *const List) -> *mut c_void {
    lfirst(list_nth_cell(l, 0))
}
/// `linitial_int(l)`.
///
/// # Safety
/// See [`linitial`]; `l` must be an IntList.
#[inline]
pub unsafe fn linitial_int(l: *const List) -> c_int {
    lfirst_int(list_nth_cell(l, 0))
}
/// `linitial_oid(l)`.
///
/// # Safety
/// See [`linitial`]; `l` must be an OidList.
#[inline]
pub unsafe fn linitial_oid(l: *const List) -> Oid {
    lfirst_oid(list_nth_cell(l, 0))
}
/// `linitial_node(type, l)`.
#[macro_export]
macro_rules! linitial_node {
    ($ty:ty, $tag:ident, $l:expr) => {
        $crate::castNode!($ty, $tag, $crate::nodes::pg_list::linitial($l))
    };
}

/// `lsecond(l)` = `lfirst(list_nth_cell(l, 1))`.
///
/// # Safety
/// `l` must be a non-NIL pointer List with at least 2 elements.
#[inline]
pub unsafe fn lsecond(l: *const List) -> *mut c_void {
    lfirst(list_nth_cell(l, 1))
}
/// `lsecond_int(l)`.
///
/// # Safety
/// See [`lsecond`]; `l` must be an IntList.
#[inline]
pub unsafe fn lsecond_int(l: *const List) -> c_int {
    lfirst_int(list_nth_cell(l, 1))
}
/// `lsecond_oid(l)`.
///
/// # Safety
/// See [`lsecond`]; `l` must be an OidList.
#[inline]
pub unsafe fn lsecond_oid(l: *const List) -> Oid {
    lfirst_oid(list_nth_cell(l, 1))
}
/// `lsecond_node(type, l)`.
#[macro_export]
macro_rules! lsecond_node {
    ($ty:ty, $tag:ident, $l:expr) => {
        $crate::castNode!($ty, $tag, $crate::nodes::pg_list::lsecond($l))
    };
}

/// `lthird(l)` = `lfirst(list_nth_cell(l, 2))`.
///
/// # Safety
/// `l` must be a non-NIL pointer List with at least 3 elements.
#[inline]
pub unsafe fn lthird(l: *const List) -> *mut c_void {
    lfirst(list_nth_cell(l, 2))
}
/// `lthird_int(l)`.
///
/// # Safety
/// See [`lthird`]; `l` must be an IntList.
#[inline]
pub unsafe fn lthird_int(l: *const List) -> c_int {
    lfirst_int(list_nth_cell(l, 2))
}
/// `lthird_oid(l)`.
///
/// # Safety
/// See [`lthird`]; `l` must be an OidList.
#[inline]
pub unsafe fn lthird_oid(l: *const List) -> Oid {
    lfirst_oid(list_nth_cell(l, 2))
}
/// `lthird_node(type, l)`.
#[macro_export]
macro_rules! lthird_node {
    ($ty:ty, $tag:ident, $l:expr) => {
        $crate::castNode!($ty, $tag, $crate::nodes::pg_list::lthird($l))
    };
}

/// `lfourth(l)` = `lfirst(list_nth_cell(l, 3))`.
///
/// # Safety
/// `l` must be a non-NIL pointer List with at least 4 elements.
#[inline]
pub unsafe fn lfourth(l: *const List) -> *mut c_void {
    lfirst(list_nth_cell(l, 3))
}
/// `lfourth_int(l)`.
///
/// # Safety
/// See [`lfourth`]; `l` must be an IntList.
#[inline]
pub unsafe fn lfourth_int(l: *const List) -> c_int {
    lfirst_int(list_nth_cell(l, 3))
}
/// `lfourth_oid(l)`.
///
/// # Safety
/// See [`lfourth`]; `l` must be an OidList.
#[inline]
pub unsafe fn lfourth_oid(l: *const List) -> Oid {
    lfirst_oid(list_nth_cell(l, 3))
}
/// `lfourth_node(type, l)`.
#[macro_export]
macro_rules! lfourth_node {
    ($ty:ty, $tag:ident, $l:expr) => {
        $crate::castNode!($ty, $tag, $crate::nodes::pg_list::lfourth($l))
    };
}

/// `llast(l)` = `lfirst(list_last_cell(l))`.
///
/// # Safety
/// `l` must be a non-NIL pointer List.
#[inline]
pub unsafe fn llast(l: *const List) -> *mut c_void {
    lfirst(list_last_cell(l))
}
/// `llast_int(l)`.
///
/// # Safety
/// See [`llast`]; `l` must be an IntList.
#[inline]
pub unsafe fn llast_int(l: *const List) -> c_int {
    lfirst_int(list_last_cell(l))
}
/// `llast_oid(l)`.
///
/// # Safety
/// See [`llast`]; `l` must be an OidList.
#[inline]
pub unsafe fn llast_oid(l: *const List) -> Oid {
    lfirst_oid(list_last_cell(l))
}
/// `llast_xid(l)`.
///
/// # Safety
/// See [`llast`]; `l` must be an XidList.
#[inline]
pub unsafe fn llast_xid(l: *const List) -> TransactionId {
    lfirst_xid(list_last_cell(l))
}
/// `llast_node(type, l)`.
#[macro_export]
macro_rules! llast_node {
    ($ty:ty, $tag:ident, $l:expr) => {
        $crate::castNode!($ty, $tag, $crate::nodes::pg_list::llast($l))
    };
}

/*
 * Convenience macros for building fixed-length lists.
 *
 * In C these construct compound-literal ListCells.  We translate them to
 * helper constructors returning a `ListCell`.
 */

/// `list_make_ptr_cell(v)` = `(ListCell) {.ptr_value = v}`.
#[inline]
pub fn list_make_ptr_cell(v: *mut c_void) -> ListCell {
    ListCell { ptr_value: v }
}
/// `list_make_int_cell(v)` = `(ListCell) {.int_value = v}`.
#[inline]
pub fn list_make_int_cell(v: c_int) -> ListCell {
    ListCell { int_value: v }
}
/// `list_make_oid_cell(v)` = `(ListCell) {.oid_value = v}`.
#[inline]
pub fn list_make_oid_cell(v: Oid) -> ListCell {
    ListCell { oid_value: v }
}
/// `list_make_xid_cell(v)` = `(ListCell) {.xid_value = v}`.
#[inline]
pub fn list_make_xid_cell(v: TransactionId) -> ListCell {
    ListCell { xid_value: v }
}

// ---- list_make1..5 (pointer) ----
#[macro_export]
macro_rules! list_make1 {
    ($x1:expr) => {
        $crate::nodes::pg_list::list_make1_impl(
            $crate::nodes::nodes::NodeTag::T_List,
            $crate::nodes::pg_list::list_make_ptr_cell($x1 as *mut core::ffi::c_void),
        )
    };
}
#[macro_export]
macro_rules! list_make2 {
    ($x1:expr, $x2:expr) => {
        $crate::nodes::pg_list::list_make2_impl(
            $crate::nodes::nodes::NodeTag::T_List,
            $crate::nodes::pg_list::list_make_ptr_cell($x1 as *mut core::ffi::c_void),
            $crate::nodes::pg_list::list_make_ptr_cell($x2 as *mut core::ffi::c_void),
        )
    };
}
#[macro_export]
macro_rules! list_make3 {
    ($x1:expr, $x2:expr, $x3:expr) => {
        $crate::nodes::pg_list::list_make3_impl(
            $crate::nodes::nodes::NodeTag::T_List,
            $crate::nodes::pg_list::list_make_ptr_cell($x1 as *mut core::ffi::c_void),
            $crate::nodes::pg_list::list_make_ptr_cell($x2 as *mut core::ffi::c_void),
            $crate::nodes::pg_list::list_make_ptr_cell($x3 as *mut core::ffi::c_void),
        )
    };
}
#[macro_export]
macro_rules! list_make4 {
    ($x1:expr, $x2:expr, $x3:expr, $x4:expr) => {
        $crate::nodes::pg_list::list_make4_impl(
            $crate::nodes::nodes::NodeTag::T_List,
            $crate::nodes::pg_list::list_make_ptr_cell($x1 as *mut core::ffi::c_void),
            $crate::nodes::pg_list::list_make_ptr_cell($x2 as *mut core::ffi::c_void),
            $crate::nodes::pg_list::list_make_ptr_cell($x3 as *mut core::ffi::c_void),
            $crate::nodes::pg_list::list_make_ptr_cell($x4 as *mut core::ffi::c_void),
        )
    };
}
#[macro_export]
macro_rules! list_make5 {
    ($x1:expr, $x2:expr, $x3:expr, $x4:expr, $x5:expr) => {
        $crate::nodes::pg_list::list_make5_impl(
            $crate::nodes::nodes::NodeTag::T_List,
            $crate::nodes::pg_list::list_make_ptr_cell($x1 as *mut core::ffi::c_void),
            $crate::nodes::pg_list::list_make_ptr_cell($x2 as *mut core::ffi::c_void),
            $crate::nodes::pg_list::list_make_ptr_cell($x3 as *mut core::ffi::c_void),
            $crate::nodes::pg_list::list_make_ptr_cell($x4 as *mut core::ffi::c_void),
            $crate::nodes::pg_list::list_make_ptr_cell($x5 as *mut core::ffi::c_void),
        )
    };
}

// ---- list_make1..5_int ----
#[macro_export]
macro_rules! list_make1_int {
    ($x1:expr) => {
        $crate::nodes::pg_list::list_make1_impl(
            $crate::nodes::nodes::NodeTag::T_IntList,
            $crate::nodes::pg_list::list_make_int_cell($x1),
        )
    };
}
#[macro_export]
macro_rules! list_make2_int {
    ($x1:expr, $x2:expr) => {
        $crate::nodes::pg_list::list_make2_impl(
            $crate::nodes::nodes::NodeTag::T_IntList,
            $crate::nodes::pg_list::list_make_int_cell($x1),
            $crate::nodes::pg_list::list_make_int_cell($x2),
        )
    };
}
#[macro_export]
macro_rules! list_make3_int {
    ($x1:expr, $x2:expr, $x3:expr) => {
        $crate::nodes::pg_list::list_make3_impl(
            $crate::nodes::nodes::NodeTag::T_IntList,
            $crate::nodes::pg_list::list_make_int_cell($x1),
            $crate::nodes::pg_list::list_make_int_cell($x2),
            $crate::nodes::pg_list::list_make_int_cell($x3),
        )
    };
}
#[macro_export]
macro_rules! list_make4_int {
    ($x1:expr, $x2:expr, $x3:expr, $x4:expr) => {
        $crate::nodes::pg_list::list_make4_impl(
            $crate::nodes::nodes::NodeTag::T_IntList,
            $crate::nodes::pg_list::list_make_int_cell($x1),
            $crate::nodes::pg_list::list_make_int_cell($x2),
            $crate::nodes::pg_list::list_make_int_cell($x3),
            $crate::nodes::pg_list::list_make_int_cell($x4),
        )
    };
}
#[macro_export]
macro_rules! list_make5_int {
    ($x1:expr, $x2:expr, $x3:expr, $x4:expr, $x5:expr) => {
        $crate::nodes::pg_list::list_make5_impl(
            $crate::nodes::nodes::NodeTag::T_IntList,
            $crate::nodes::pg_list::list_make_int_cell($x1),
            $crate::nodes::pg_list::list_make_int_cell($x2),
            $crate::nodes::pg_list::list_make_int_cell($x3),
            $crate::nodes::pg_list::list_make_int_cell($x4),
            $crate::nodes::pg_list::list_make_int_cell($x5),
        )
    };
}

// ---- list_make1..5_oid ----
#[macro_export]
macro_rules! list_make1_oid {
    ($x1:expr) => {
        $crate::nodes::pg_list::list_make1_impl(
            $crate::nodes::nodes::NodeTag::T_OidList,
            $crate::nodes::pg_list::list_make_oid_cell($x1),
        )
    };
}
#[macro_export]
macro_rules! list_make2_oid {
    ($x1:expr, $x2:expr) => {
        $crate::nodes::pg_list::list_make2_impl(
            $crate::nodes::nodes::NodeTag::T_OidList,
            $crate::nodes::pg_list::list_make_oid_cell($x1),
            $crate::nodes::pg_list::list_make_oid_cell($x2),
        )
    };
}
#[macro_export]
macro_rules! list_make3_oid {
    ($x1:expr, $x2:expr, $x3:expr) => {
        $crate::nodes::pg_list::list_make3_impl(
            $crate::nodes::nodes::NodeTag::T_OidList,
            $crate::nodes::pg_list::list_make_oid_cell($x1),
            $crate::nodes::pg_list::list_make_oid_cell($x2),
            $crate::nodes::pg_list::list_make_oid_cell($x3),
        )
    };
}
#[macro_export]
macro_rules! list_make4_oid {
    ($x1:expr, $x2:expr, $x3:expr, $x4:expr) => {
        $crate::nodes::pg_list::list_make4_impl(
            $crate::nodes::nodes::NodeTag::T_OidList,
            $crate::nodes::pg_list::list_make_oid_cell($x1),
            $crate::nodes::pg_list::list_make_oid_cell($x2),
            $crate::nodes::pg_list::list_make_oid_cell($x3),
            $crate::nodes::pg_list::list_make_oid_cell($x4),
        )
    };
}
#[macro_export]
macro_rules! list_make5_oid {
    ($x1:expr, $x2:expr, $x3:expr, $x4:expr, $x5:expr) => {
        $crate::nodes::pg_list::list_make5_impl(
            $crate::nodes::nodes::NodeTag::T_OidList,
            $crate::nodes::pg_list::list_make_oid_cell($x1),
            $crate::nodes::pg_list::list_make_oid_cell($x2),
            $crate::nodes::pg_list::list_make_oid_cell($x3),
            $crate::nodes::pg_list::list_make_oid_cell($x4),
            $crate::nodes::pg_list::list_make_oid_cell($x5),
        )
    };
}

// ---- list_make1..5_xid ----
#[macro_export]
macro_rules! list_make1_xid {
    ($x1:expr) => {
        $crate::nodes::pg_list::list_make1_impl(
            $crate::nodes::nodes::NodeTag::T_XidList,
            $crate::nodes::pg_list::list_make_xid_cell($x1),
        )
    };
}
#[macro_export]
macro_rules! list_make2_xid {
    ($x1:expr, $x2:expr) => {
        $crate::nodes::pg_list::list_make2_impl(
            $crate::nodes::nodes::NodeTag::T_XidList,
            $crate::nodes::pg_list::list_make_xid_cell($x1),
            $crate::nodes::pg_list::list_make_xid_cell($x2),
        )
    };
}
#[macro_export]
macro_rules! list_make3_xid {
    ($x1:expr, $x2:expr, $x3:expr) => {
        $crate::nodes::pg_list::list_make3_impl(
            $crate::nodes::nodes::NodeTag::T_XidList,
            $crate::nodes::pg_list::list_make_xid_cell($x1),
            $crate::nodes::pg_list::list_make_xid_cell($x2),
            $crate::nodes::pg_list::list_make_xid_cell($x3),
        )
    };
}
#[macro_export]
macro_rules! list_make4_xid {
    ($x1:expr, $x2:expr, $x3:expr, $x4:expr) => {
        $crate::nodes::pg_list::list_make4_impl(
            $crate::nodes::nodes::NodeTag::T_XidList,
            $crate::nodes::pg_list::list_make_xid_cell($x1),
            $crate::nodes::pg_list::list_make_xid_cell($x2),
            $crate::nodes::pg_list::list_make_xid_cell($x3),
            $crate::nodes::pg_list::list_make_xid_cell($x4),
        )
    };
}
#[macro_export]
macro_rules! list_make5_xid {
    ($x1:expr, $x2:expr, $x3:expr, $x4:expr, $x5:expr) => {
        $crate::nodes::pg_list::list_make5_impl(
            $crate::nodes::nodes::NodeTag::T_XidList,
            $crate::nodes::pg_list::list_make_xid_cell($x1),
            $crate::nodes::pg_list::list_make_xid_cell($x2),
            $crate::nodes::pg_list::list_make_xid_cell($x3),
            $crate::nodes::pg_list::list_make_xid_cell($x4),
            $crate::nodes::pg_list::list_make_xid_cell($x5),
        )
    };
}

/// Locate the n'th cell (counting from 0) of the list.
/// It is an assertion failure if there is no such cell.
///
/// # Safety
/// `list` must be non-NIL and `n` in range.
#[inline]
pub unsafe fn list_nth_cell(list: *const List, n: c_int) -> *mut ListCell {
    Assert!(list != NIL);
    Assert!(n >= 0 && n < (*list).length);
    (*list).elements.add(n as usize)
}

/// Return the last cell in a non-NIL List.
///
/// # Safety
/// `list` must be non-NIL.
#[inline]
pub unsafe fn list_last_cell(list: *const List) -> *mut ListCell {
    Assert!(list != NIL);
    (*list).elements.add(((*list).length - 1) as usize)
}

/// Return the pointer value contained in the n'th element of the
/// specified list. (List elements begin at 0.)
///
/// # Safety
/// `list` must be a non-NIL pointer List with `n` in range.
#[inline]
pub unsafe fn list_nth(list: *const List, n: c_int) -> *mut c_void {
    Assert!(IsA!(list, T_List));
    lfirst(list_nth_cell(list, n))
}

/// Return the integer value contained in the n'th element of the
/// specified list.
///
/// # Safety
/// `list` must be a non-NIL IntList with `n` in range.
#[inline]
pub unsafe fn list_nth_int(list: *const List, n: c_int) -> c_int {
    Assert!(IsA!(list, T_IntList));
    lfirst_int(list_nth_cell(list, n))
}

/// Return the OID value contained in the n'th element of the specified list.
///
/// # Safety
/// `list` must be a non-NIL OidList with `n` in range.
#[inline]
pub unsafe fn list_nth_oid(list: *const List, n: c_int) -> Oid {
    Assert!(IsA!(list, T_OidList));
    lfirst_oid(list_nth_cell(list, n))
}

/// `list_nth_node(type, list, n)` = `castNode(type, list_nth(list, n))`.
#[macro_export]
macro_rules! list_nth_node {
    ($ty:ty, $tag:ident, $list:expr, $n:expr) => {
        $crate::castNode!($ty, $tag, $crate::nodes::pg_list::list_nth($list, $n))
    };
}

/// Get the given ListCell's index (from 0) in the given List.
///
/// # Safety
/// `c` must point within `l`'s element array.
#[inline]
pub unsafe fn list_cell_number(l: *const List, c: *const ListCell) -> c_int {
    Assert!(c >= (*l).elements && c < (*l).elements.add((*l).length as usize));
    c.offset_from((*l).elements) as c_int
}

/// Get the address of the next cell after "c" within list "l", or NULL if none.
///
/// # Safety
/// `c` must point within `l`'s element array.
#[inline]
pub unsafe fn lnext(l: *const List, c: *const ListCell) -> *mut ListCell {
    Assert!(c >= (*l).elements && c < (*l).elements.add((*l).length as usize));
    let c = c.add(1);
    if c < (*l).elements.add((*l).length as usize) {
        c as *mut ListCell
    } else {
        core::ptr::null_mut()
    }
}

// ===========================================================================
//                  The looping macros (trailing-block form)
// ===========================================================================

/*
 * foreach -
 *    a convenience macro for looping through a list.
 *
 * C signature: foreach(cell, lst) { ... }
 * Rust:        foreach!(cell, lst, { ... });
 *
 * STABLE-RUST DEVIATION (loop variable is a cursor, not a bare pointer):
 *
 * C builds the iterator state in a variable named `cell##__state` via token
 * pasting, with a separate `cell` ListCell* iteration pointer.  Stable Rust
 * cannot synthesize the `cell##__state` identifier, and macro hygiene prevents a
 * fixed-name hidden binding in `foreach!` from being seen by the *separate*
 * `foreach_delete_current!`/`foreach_current_index!` macros.  The only token that
 * flows through all three macros is the loop variable name itself.
 *
 * Therefore the loop variable `cell` is bound to a `ForEachState` cursor (carrying
 * the list, the index, and the current cell pointer), under the caller's token.
 * Inside the body, obtain the current `*mut ListCell` with `current_cell!(cell)`
 * (and `lfirst`/`lfirst_int`/... accept that pointer as in C).  This lets
 * `foreach_delete_current!`/`foreach_current_index!` reach the same cursor by
 * naming the same token.
 *
 * Beware of changing the List object while the loop is iterating (see the
 * original comment in pg_list.h).
 */
#[macro_export]
macro_rules! foreach {
    ($cell:ident, $lst:expr, $body:block) => {{
        #[allow(unused_mut, unused_variables)]
        let mut $cell: $crate::nodes::pg_list::ForEachState =
            $crate::nodes::pg_list::ForEachState { l: $lst, i: 0 };
        loop {
            if !(!$cell.l.is_null() && $cell.i < (*$cell.l).length) {
                break;
            }
            $body
            $cell.i += 1;
        }
    }};
}

/// `current_cell!(cell)` - the `*mut ListCell` at the cursor's current position.
///
/// Replaces the C convention where the `foreach` loop variable is itself the cell
/// pointer.  Use inside a `foreach!`/`for_each_from!`/`for_each_cell!` body.
#[macro_export]
macro_rules! current_cell {
    ($cell:ident) => {
        (*$cell.l).elements.add($cell.i as usize)
    };
}

/*
 * foreach_delete_current -
 *    delete the current list element from the List associated with a
 *    surrounding foreach()/foreach_*() loop, returning the new List pointer.
 *
 * C: foreach_delete_current(lst, cell)
 * Rust: foreach_delete_current!(lst, cell)   // must be called inside the loop body
 *
 * As in C, this adjusts the iterator state so that no elements are missed on the
 * next iteration; here the state is the `cell` cursor itself.
 */
#[macro_export]
macro_rules! foreach_delete_current {
    ($lst:expr, $cell:ident) => {{
        let __i = $cell.i;
        $cell.i -= 1;
        let __newl = $crate::nodes::pg_list::list_delete_nth_cell($lst, __i);
        $cell.l = __newl;
        __newl
    }};
}

/*
 * foreach_current_index -
 *    get the zero-based list index of a surrounding foreach()/foreach_*()
 *    loop's current element.  Must be called inside the loop body.
 */
#[macro_export]
macro_rules! foreach_current_index {
    ($cell:ident) => {
        $cell.i
    };
}

/// `for_each_from_setup(lst, N)` -> ForEachState{lst, N}, asserting N >= 0.
///
/// # Safety
/// `lst` must be NIL or a valid List.
#[inline]
pub unsafe fn for_each_from_setup(lst: *const List, n: c_int) -> ForEachState {
    let r = ForEachState { l: lst, i: n };
    Assert!(n >= 0);
    r
}

/*
 * for_each_from -
 *    Like foreach(), but start from the N'th (zero-based) list element.
 *
 * C: for_each_from(cell, lst, N) { ... }
 * Rust: for_each_from!(cell, lst, N, { ... });
 */
#[macro_export]
macro_rules! for_each_from {
    ($cell:ident, $lst:expr, $n:expr, $body:block) => {{
        #[allow(unused_mut, unused_variables)]
        let mut $cell: $crate::nodes::pg_list::ForEachState =
            $crate::nodes::pg_list::for_each_from_setup($lst, $n);
        loop {
            if !(!$cell.l.is_null() && $cell.i < (*$cell.l).length) {
                break;
            }
            $body
            $cell.i += 1;
        }
    }};
}

/// `for_each_cell_setup(lst, initcell)`.
///
/// # Safety
/// `lst` must be NIL or a valid List; `initcell` must be NULL or within `lst`.
#[inline]
pub unsafe fn for_each_cell_setup(lst: *const List, initcell: *const ListCell) -> ForEachState {
    ForEachState {
        l: lst,
        i: if !initcell.is_null() {
            list_cell_number(lst, initcell)
        } else {
            list_length(lst)
        },
    }
}

/*
 * for_each_cell -
 *    a convenience macro which loops through a list starting from a
 *    specified cell.
 *
 * C: for_each_cell(cell, lst, initcell) { ... }
 * Rust: for_each_cell!(cell, lst, initcell, { ... });
 */
#[macro_export]
macro_rules! for_each_cell {
    ($cell:ident, $lst:expr, $initcell:expr, $body:block) => {{
        #[allow(unused_mut, unused_variables)]
        let mut $cell: $crate::nodes::pg_list::ForEachState =
            $crate::nodes::pg_list::for_each_cell_setup($lst, $initcell);
        loop {
            if !(!$cell.l.is_null() && $cell.i < (*$cell.l).length) {
                break;
            }
            $body
            $cell.i += 1;
        }
    }};
}

/*
 * Typed convenience loops (declare a typed loop variable).
 *
 * C: foreach_ptr(type, var, lst) / foreach_int(var, lst) /
 *    foreach_oid(var, lst) / foreach_xid(var, lst) / foreach_node(type, var, lst)
 *
 * Rust: foreach_ptr!(Type, var, lst, { ... });  // var: *mut Type
 *       foreach_int!(var, lst, { ... });         // var: c_int
 *       foreach_oid!(var, lst, { ... });         // var: Oid
 *       foreach_xid!(var, lst, { ... });         // var: TransactionId
 *       foreach_node!(Type, T_Type, var, lst, { ... }); // var: *mut Type, tag-checked
 *
 * Note: the iterator state is named `<var>__state`, as for foreach!.
 */
#[macro_export]
macro_rules! foreach_ptr {
    ($ty:ty, $var:ident, $lst:expr, $body:block) => {{
        #[allow(unused_mut)]
        let mut $var: *mut $ty = core::ptr::null_mut();
        #[allow(unused_mut, unused_variables)]
        let mut __pg_foreach_state: $crate::nodes::pg_list::ForEachState =
            $crate::nodes::pg_list::ForEachState { l: $lst, i: 0 };
        loop {
            if !(!__pg_foreach_state.l.is_null()
                && __pg_foreach_state.i < (*__pg_foreach_state.l).length)
            {
                break;
            }
            $var = $crate::nodes::pg_list::lfirst(
                (*__pg_foreach_state.l).elements.add(__pg_foreach_state.i as usize),
            ) as *mut $ty;
            $body
            __pg_foreach_state.i += 1;
        }
        let _ = &$var;
    }};
}
#[macro_export]
macro_rules! foreach_int {
    ($var:ident, $lst:expr, $body:block) => {{
        #[allow(unused_mut)]
        let mut $var: core::ffi::c_int = 0;
        #[allow(unused_mut, unused_variables)]
        let mut __pg_foreach_state: $crate::nodes::pg_list::ForEachState =
            $crate::nodes::pg_list::ForEachState { l: $lst, i: 0 };
        loop {
            if !(!__pg_foreach_state.l.is_null()
                && __pg_foreach_state.i < (*__pg_foreach_state.l).length)
            {
                break;
            }
            $var = $crate::nodes::pg_list::lfirst_int(
                (*__pg_foreach_state.l).elements.add(__pg_foreach_state.i as usize),
            );
            $body
            __pg_foreach_state.i += 1;
        }
        let _ = &$var;
    }};
}
#[macro_export]
macro_rules! foreach_oid {
    ($var:ident, $lst:expr, $body:block) => {{
        #[allow(unused_mut)]
        let mut $var: $crate::Oid = 0;
        #[allow(unused_mut, unused_variables)]
        let mut __pg_foreach_state: $crate::nodes::pg_list::ForEachState =
            $crate::nodes::pg_list::ForEachState { l: $lst, i: 0 };
        loop {
            if !(!__pg_foreach_state.l.is_null()
                && __pg_foreach_state.i < (*__pg_foreach_state.l).length)
            {
                break;
            }
            $var = $crate::nodes::pg_list::lfirst_oid(
                (*__pg_foreach_state.l).elements.add(__pg_foreach_state.i as usize),
            );
            $body
            __pg_foreach_state.i += 1;
        }
        let _ = &$var;
    }};
}
#[macro_export]
macro_rules! foreach_xid {
    ($var:ident, $lst:expr, $body:block) => {{
        #[allow(unused_mut)]
        let mut $var: $crate::TransactionId = 0;
        #[allow(unused_mut, unused_variables)]
        let mut __pg_foreach_state: $crate::nodes::pg_list::ForEachState =
            $crate::nodes::pg_list::ForEachState { l: $lst, i: 0 };
        loop {
            if !(!__pg_foreach_state.l.is_null()
                && __pg_foreach_state.i < (*__pg_foreach_state.l).length)
            {
                break;
            }
            $var = $crate::nodes::pg_list::lfirst_xid(
                (*__pg_foreach_state.l).elements.add(__pg_foreach_state.i as usize),
            );
            $body
            __pg_foreach_state.i += 1;
        }
        let _ = &$var;
    }};
}
#[macro_export]
macro_rules! foreach_node {
    ($ty:ty, $tag:ident, $var:ident, $lst:expr, $body:block) => {{
        #[allow(unused_mut)]
        let mut $var: *mut $ty = core::ptr::null_mut();
        #[allow(unused_mut, unused_variables)]
        let mut __pg_foreach_state: $crate::nodes::pg_list::ForEachState =
            $crate::nodes::pg_list::ForEachState { l: $lst, i: 0 };
        loop {
            if !(!__pg_foreach_state.l.is_null()
                && __pg_foreach_state.i < (*__pg_foreach_state.l).length)
            {
                break;
            }
            $var = $crate::lfirst_node!(
                $ty,
                $tag,
                (*__pg_foreach_state.l).elements.add(__pg_foreach_state.i as usize)
            );
            $body
            __pg_foreach_state.i += 1;
        }
        let _ = &$var;
    }};
}

/*
 * forboth -
 *    advance through two lists simultaneously, stopping when either runs out.
 *
 * C: forboth(cell1, list1, cell2, list2) { ... }
 * Rust: forboth!(cell1, list1, cell2, list2, { ... });
 *
 * cell1/cell2 are `*mut ListCell` (NULL when that list is exhausted).  As in C,
 * some callers rely on the ending cell values being separately NULL/non-NULL.
 * The shared state lives in `<cell1>__state` (a ForBothState).
 */
#[macro_export]
macro_rules! forboth {
    ($cell1:ident, $list1:expr, $cell2:ident, $list2:expr, $body:block) => {{
        #[allow(unused_mut, unused_assignments)]
        let mut $cell1: *mut $crate::nodes::pg_list::ListCell = core::ptr::null_mut();
        #[allow(unused_mut, unused_assignments)]
        let mut $cell2: *mut $crate::nodes::pg_list::ListCell = core::ptr::null_mut();
        let mut __pg_multifor_state: $crate::nodes::pg_list::ForBothState =
            $crate::nodes::pg_list::ForBothState { l1: $list1, l2: $list2, i: 0 };
        loop {
            let st = __pg_multifor_state;
            $cell1 = if !st.l1.is_null() && st.i < (*st.l1).length {
                (*st.l1).elements.add(st.i as usize)
            } else {
                core::ptr::null_mut()
            };
            $cell2 = if !st.l2.is_null() && st.i < (*st.l2).length {
                (*st.l2).elements.add(st.i as usize)
            } else {
                core::ptr::null_mut()
            };
            if !(!$cell1.is_null() && !$cell2.is_null()) {
                break;
            }
            $body
            __pg_multifor_state.i += 1;
        }
        let _ = (&$cell1, &$cell2);
    }};
}

/// `for_both_cell_setup(list1, initcell1, list2, initcell2)`.
///
/// # Safety
/// Each list must be NIL or valid; each initcell NULL or within its list.
#[inline]
pub unsafe fn for_both_cell_setup(
    list1: *const List,
    initcell1: *const ListCell,
    list2: *const List,
    initcell2: *const ListCell,
) -> ForBothCellState {
    ForBothCellState {
        l1: list1,
        l2: list2,
        i1: if !initcell1.is_null() {
            list_cell_number(list1, initcell1)
        } else {
            list_length(list1)
        },
        i2: if !initcell2.is_null() {
            list_cell_number(list2, initcell2)
        } else {
            list_length(list2)
        },
    }
}

/*
 * for_both_cell -
 *    loop through two lists starting from the specified cells of each.
 *
 * C: for_both_cell(cell1, list1, initcell1, cell2, list2, initcell2) { ... }
 * Rust: for_both_cell!(cell1, list1, initcell1, cell2, list2, initcell2, { ... });
 */
#[macro_export]
macro_rules! for_both_cell {
    ($cell1:ident, $list1:expr, $initcell1:expr,
     $cell2:ident, $list2:expr, $initcell2:expr, $body:block) => {{
        #[allow(unused_mut, unused_assignments)]
        let mut $cell1: *mut $crate::nodes::pg_list::ListCell = core::ptr::null_mut();
        #[allow(unused_mut, unused_assignments)]
        let mut $cell2: *mut $crate::nodes::pg_list::ListCell = core::ptr::null_mut();
        let mut __pg_multifor_state: $crate::nodes::pg_list::ForBothCellState =
            $crate::nodes::pg_list::for_both_cell_setup($list1, $initcell1, $list2, $initcell2);
        loop {
            let st = __pg_multifor_state;
            $cell1 = if !st.l1.is_null() && st.i1 < (*st.l1).length {
                (*st.l1).elements.add(st.i1 as usize)
            } else {
                core::ptr::null_mut()
            };
            $cell2 = if !st.l2.is_null() && st.i2 < (*st.l2).length {
                (*st.l2).elements.add(st.i2 as usize)
            } else {
                core::ptr::null_mut()
            };
            if !(!$cell1.is_null() && !$cell2.is_null()) {
                break;
            }
            $body
            __pg_multifor_state.i1 += 1;
            __pg_multifor_state.i2 += 1;
        }
        let _ = (&$cell1, &$cell2);
    }};
}

/*
 * forthree - the same for three lists.
 *
 * Rust: forthree!(cell1, list1, cell2, list2, cell3, list3, { ... });
 */
#[macro_export]
macro_rules! forthree {
    ($cell1:ident, $list1:expr, $cell2:ident, $list2:expr,
     $cell3:ident, $list3:expr, $body:block) => {{
        #[allow(unused_mut, unused_assignments)]
        let mut $cell1: *mut $crate::nodes::pg_list::ListCell = core::ptr::null_mut();
        #[allow(unused_mut, unused_assignments)]
        let mut $cell2: *mut $crate::nodes::pg_list::ListCell = core::ptr::null_mut();
        #[allow(unused_mut, unused_assignments)]
        let mut $cell3: *mut $crate::nodes::pg_list::ListCell = core::ptr::null_mut();
        let mut __pg_multifor_state: $crate::nodes::pg_list::ForThreeState =
            $crate::nodes::pg_list::ForThreeState { l1: $list1, l2: $list2, l3: $list3, i: 0 };
        loop {
            let st = __pg_multifor_state;
            $cell1 = if !st.l1.is_null() && st.i < (*st.l1).length {
                (*st.l1).elements.add(st.i as usize)
            } else { core::ptr::null_mut() };
            $cell2 = if !st.l2.is_null() && st.i < (*st.l2).length {
                (*st.l2).elements.add(st.i as usize)
            } else { core::ptr::null_mut() };
            $cell3 = if !st.l3.is_null() && st.i < (*st.l3).length {
                (*st.l3).elements.add(st.i as usize)
            } else { core::ptr::null_mut() };
            if !(!$cell1.is_null() && !$cell2.is_null() && !$cell3.is_null()) {
                break;
            }
            $body
            __pg_multifor_state.i += 1;
        }
        let _ = (&$cell1, &$cell2, &$cell3);
    }};
}

/*
 * forfour - the same for four lists.
 *
 * Rust: forfour!(cell1, list1, cell2, list2, cell3, list3, cell4, list4, { ... });
 */
#[macro_export]
macro_rules! forfour {
    ($cell1:ident, $list1:expr, $cell2:ident, $list2:expr,
     $cell3:ident, $list3:expr, $cell4:ident, $list4:expr, $body:block) => {{
        #[allow(unused_mut, unused_assignments)]
        let mut $cell1: *mut $crate::nodes::pg_list::ListCell = core::ptr::null_mut();
        #[allow(unused_mut, unused_assignments)]
        let mut $cell2: *mut $crate::nodes::pg_list::ListCell = core::ptr::null_mut();
        #[allow(unused_mut, unused_assignments)]
        let mut $cell3: *mut $crate::nodes::pg_list::ListCell = core::ptr::null_mut();
        #[allow(unused_mut, unused_assignments)]
        let mut $cell4: *mut $crate::nodes::pg_list::ListCell = core::ptr::null_mut();
        let mut __pg_multifor_state: $crate::nodes::pg_list::ForFourState =
            $crate::nodes::pg_list::ForFourState {
                l1: $list1, l2: $list2, l3: $list3, l4: $list4, i: 0,
            };
        loop {
            let st = __pg_multifor_state;
            $cell1 = if !st.l1.is_null() && st.i < (*st.l1).length {
                (*st.l1).elements.add(st.i as usize)
            } else { core::ptr::null_mut() };
            $cell2 = if !st.l2.is_null() && st.i < (*st.l2).length {
                (*st.l2).elements.add(st.i as usize)
            } else { core::ptr::null_mut() };
            $cell3 = if !st.l3.is_null() && st.i < (*st.l3).length {
                (*st.l3).elements.add(st.i as usize)
            } else { core::ptr::null_mut() };
            $cell4 = if !st.l4.is_null() && st.i < (*st.l4).length {
                (*st.l4).elements.add(st.i as usize)
            } else { core::ptr::null_mut() };
            if !(!$cell1.is_null() && !$cell2.is_null()
                 && !$cell3.is_null() && !$cell4.is_null()) {
                break;
            }
            $body
            __pg_multifor_state.i += 1;
        }
        let _ = (&$cell1, &$cell2, &$cell3, &$cell4);
    }};
}

/*
 * forfive - the same for five lists.
 *
 * Rust: forfive!(c1, l1, c2, l2, c3, l3, c4, l4, c5, l5, { ... });
 */
#[macro_export]
macro_rules! forfive {
    ($cell1:ident, $list1:expr, $cell2:ident, $list2:expr,
     $cell3:ident, $list3:expr, $cell4:ident, $list4:expr,
     $cell5:ident, $list5:expr, $body:block) => {{
        #[allow(unused_mut, unused_assignments)]
        let mut $cell1: *mut $crate::nodes::pg_list::ListCell = core::ptr::null_mut();
        #[allow(unused_mut, unused_assignments)]
        let mut $cell2: *mut $crate::nodes::pg_list::ListCell = core::ptr::null_mut();
        #[allow(unused_mut, unused_assignments)]
        let mut $cell3: *mut $crate::nodes::pg_list::ListCell = core::ptr::null_mut();
        #[allow(unused_mut, unused_assignments)]
        let mut $cell4: *mut $crate::nodes::pg_list::ListCell = core::ptr::null_mut();
        #[allow(unused_mut, unused_assignments)]
        let mut $cell5: *mut $crate::nodes::pg_list::ListCell = core::ptr::null_mut();
        let mut __pg_multifor_state: $crate::nodes::pg_list::ForFiveState =
            $crate::nodes::pg_list::ForFiveState {
                l1: $list1, l2: $list2, l3: $list3, l4: $list4, l5: $list5, i: 0,
            };
        loop {
            let st = __pg_multifor_state;
            $cell1 = if !st.l1.is_null() && st.i < (*st.l1).length {
                (*st.l1).elements.add(st.i as usize)
            } else { core::ptr::null_mut() };
            $cell2 = if !st.l2.is_null() && st.i < (*st.l2).length {
                (*st.l2).elements.add(st.i as usize)
            } else { core::ptr::null_mut() };
            $cell3 = if !st.l3.is_null() && st.i < (*st.l3).length {
                (*st.l3).elements.add(st.i as usize)
            } else { core::ptr::null_mut() };
            $cell4 = if !st.l4.is_null() && st.i < (*st.l4).length {
                (*st.l4).elements.add(st.i as usize)
            } else { core::ptr::null_mut() };
            $cell5 = if !st.l5.is_null() && st.i < (*st.l5).length {
                (*st.l5).elements.add(st.i as usize)
            } else { core::ptr::null_mut() };
            if !(!$cell1.is_null() && !$cell2.is_null() && !$cell3.is_null()
                 && !$cell4.is_null() && !$cell5.is_null()) {
                break;
            }
            $body
            __pg_multifor_state.i += 1;
        }
        let _ = (&$cell1, &$cell2, &$cell3, &$cell4, &$cell5);
    }};
}

// ===========================================================================
//                          list.c: implementation
// ===========================================================================

/*
 * The previous List implementation, since it used a separate palloc chunk
 * for each cons cell, had the property that adding or deleting list cells
 * did not move the storage of other existing cells in the list.  Quite a
 * bit of existing code depended on that, by retaining ListCell pointers
 * across such operations on a list.  There is no such guarantee in this
 * implementation, so instead we have debugging support that is meant to
 * help flush out now-broken assumptions.  Defining DEBUG_LIST_MEMORY_USAGE
 * while building this file causes the List operations to forcibly move
 * all cells in a list whenever a cell is added or deleted.  In combination
 * with MEMORY_CONTEXT_CHECKING and/or Valgrind, this can usually expose
 * broken code.
 *
 * The DEBUG_LIST_MEMORY_USAGE / CLOBBER_FREED_MEMORY / Valgrind code paths from
 * list.c are conditionally compiled in C; we translate only the default
 * (non-debug) path here, mirroring a build without those macros defined.
 */

/// Overhead for the fixed part of a List header, measured in ListCells.
///
/// `LIST_HEADER_OVERHEAD = (offsetof(List, initial_elements) - 1) / sizeof(ListCell) + 1`.
#[inline]
const fn list_header_overhead() -> c_int {
    let off = core::mem::offset_of!(List, initial_elements);
    let cell = core::mem::size_of::<ListCell>();
    ((off - 1) / cell + 1) as c_int
}

/*
 * Macros to simplify writing assertions about the type of a list; a
 * NIL list is considered to be an empty list of any type.
 *
 * Translated as inline helpers (only used inside Assert!).
 */
#[inline]
unsafe fn is_pointer_list(l: *const List) -> bool {
    l == NIL || IsA!(l, T_List)
}
#[inline]
unsafe fn is_integer_list(l: *const List) -> bool {
    l == NIL || IsA!(l, T_IntList)
}
#[inline]
unsafe fn is_oid_list(l: *const List) -> bool {
    l == NIL || IsA!(l, T_OidList)
}
#[inline]
unsafe fn is_xid_list(l: *const List) -> bool {
    l == NIL || IsA!(l, T_XidList)
}

/*
 * Check that the specified List is valid (so far as we can tell).
 *
 * In C this is compiled only under USE_ASSERT_CHECKING.  Here it is always
 * present but its body uses Assert!, which is itself a no-op in release builds.
 *
 * # Safety
 * `list` must be NIL or a valid List.
 */
#[inline]
unsafe fn check_list_invariants(list: *const List) {
    if list == NIL {
        return;
    }

    Assert!((*list).length > 0);
    Assert!((*list).length <= (*list).max_length);
    Assert!(!(*list).elements.is_null());

    Assert!(
        (*list).r#type == T_List
            || (*list).r#type == T_IntList
            || (*list).r#type == T_OidList
            || (*list).r#type == T_XidList
    );
}

/// Return a pointer to a List's co-allocated `initial_elements[0]`.
///
/// Implements the single-allocation trick: `elements` is pointed at the cells
/// that follow the header within the same palloc chunk.
///
/// # Safety
/// `list` must point to a List whose chunk has room for `initial_elements`.
#[inline]
unsafe fn initial_elements_ptr(list: *mut List) -> *mut ListCell {
    (list as *mut u8).add(core::mem::offset_of!(List, initial_elements)) as *mut ListCell
}

/*
 * Return a freshly allocated List with room for at least min_size cells.
 *
 * Since empty non-NIL lists are invalid, new_list() sets the initial length
 * to min_size, effectively marking that number of cells as valid; the caller
 * is responsible for filling in their data.
 */
unsafe fn new_list(r#type: NodeTag, min_size: c_int) -> *mut List {
    let newlist: *mut List;
    let max_size: c_int;

    Assert!(min_size > 0);

    /*
     * We allocate all the requested cells, and possibly some more, as part of
     * the same palloc request as the List header.  This is a big win for the
     * typical case of short fixed-length lists.  It can lose if we allocate a
     * moderately long list and then it gets extended; we'll be wasting more
     * initial_elements[] space than if we'd made the header small.  However,
     * rounding up the request as we do in the normal code path provides some
     * defense against small extensions.
     */

    /*
     * Normally, we set up a list with some extra cells, to allow it to grow
     * without a repalloc.  Prefer cell counts chosen to make the total
     * allocation a power-of-2, since palloc would round it up to that anyway.
     * (That stops being true for very large allocations, but very long lists
     * are infrequent, so it doesn't seem worth special logic for such cases.)
     *
     * The minimum allocation is 8 ListCell units, providing either 4 or 5
     * available ListCells depending on the machine's word width.  Counting
     * palloc's overhead, this uses the same amount of space as a one-cell
     * list did in the old implementation, and less space for any longer list.
     *
     * We needn't worry about integer overflow; no caller passes min_size
     * that's more than twice the size of an existing list, so the size limits
     * within palloc will ensure that we don't overflow here.
     */
    let mut max_size_local =
        pg_nextpower2_32(Max(8, min_size + list_header_overhead()) as uint32) as c_int;
    max_size_local -= list_header_overhead();
    max_size = max_size_local;

    newlist = palloc(
        core::mem::offset_of!(List, initial_elements)
            + max_size as usize * core::mem::size_of::<ListCell>(),
    ) as *mut List;
    (*newlist).r#type = r#type;
    (*newlist).length = min_size;
    (*newlist).max_length = max_size;
    (*newlist).elements = initial_elements_ptr(newlist);

    newlist
}

/*
 * Enlarge an existing non-NIL List to have room for at least min_size cells.
 *
 * This does *not* update list->length, as some callers would find that
 * inconvenient.  (list->length had better be the correct number of existing
 * valid cells, though.)
 */
unsafe fn enlarge_list(list: *mut List, min_size: c_int) {
    let new_max_len: c_int;

    Assert!(min_size > (*list).max_length); /* else we shouldn't be here */

    /*
     * As above, we prefer power-of-two total allocations; but here we need
     * not account for list header overhead.
     */

    /* clamp the minimum value to 16, a semi-arbitrary small power of 2 */
    new_max_len = pg_nextpower2_32(Max(16, min_size) as uint32) as c_int;

    if (*list).elements == initial_elements_ptr(list) {
        /*
         * Replace original in-line allocation with a separate palloc block.
         * Ensure it is in the same memory context as the List header.  (The
         * previous List implementation did not offer any guarantees about
         * keeping all list cells in the same context, but it seems reasonable
         * to create such a guarantee now.)
         */
        (*list).elements = MemoryContextAlloc(
            GetMemoryChunkContext(list as *mut c_void),
            new_max_len as usize * core::mem::size_of::<ListCell>(),
        ) as *mut ListCell;
        core::ptr::copy_nonoverlapping(
            initial_elements_ptr(list),
            (*list).elements,
            (*list).length as usize,
        );

        /*
         * We must not move the list header, so it's unsafe to try to reclaim
         * the initial_elements[] space via repalloc.  In debugging builds,
         * however, we can clear that space and/or mark it inaccessible.
         * (Only the default build path is translated; nothing to do here.)
         */
    } else {
        /* Normally, let repalloc deal with enlargement */
        (*list).elements = repalloc(
            (*list).elements as *mut c_void,
            new_max_len as usize * core::mem::size_of::<ListCell>(),
        ) as *mut ListCell;
    }

    (*list).max_length = new_max_len;
}

/*
 * Convenience functions to construct short Lists from given values.
 * (These are normally invoked via the list_makeN macros.)
 */

/// `list_make1_impl(t, datum1)`.
///
/// # Safety
/// Allocates a List; the resulting pointer must be managed like any List.
pub unsafe fn list_make1_impl(t: NodeTag, datum1: ListCell) -> *mut List {
    let list = new_list(t, 1);

    *(*list).elements.add(0) = datum1;
    check_list_invariants(list);
    list
}

/// `list_make2_impl(t, datum1, datum2)`.
///
/// # Safety
/// See [`list_make1_impl`].
pub unsafe fn list_make2_impl(t: NodeTag, datum1: ListCell, datum2: ListCell) -> *mut List {
    let list = new_list(t, 2);

    *(*list).elements.add(0) = datum1;
    *(*list).elements.add(1) = datum2;
    check_list_invariants(list);
    list
}

/// `list_make3_impl(t, datum1, datum2, datum3)`.
///
/// # Safety
/// See [`list_make1_impl`].
pub unsafe fn list_make3_impl(
    t: NodeTag,
    datum1: ListCell,
    datum2: ListCell,
    datum3: ListCell,
) -> *mut List {
    let list = new_list(t, 3);

    *(*list).elements.add(0) = datum1;
    *(*list).elements.add(1) = datum2;
    *(*list).elements.add(2) = datum3;
    check_list_invariants(list);
    list
}

/// `list_make4_impl(t, datum1, datum2, datum3, datum4)`.
///
/// # Safety
/// See [`list_make1_impl`].
pub unsafe fn list_make4_impl(
    t: NodeTag,
    datum1: ListCell,
    datum2: ListCell,
    datum3: ListCell,
    datum4: ListCell,
) -> *mut List {
    let list = new_list(t, 4);

    *(*list).elements.add(0) = datum1;
    *(*list).elements.add(1) = datum2;
    *(*list).elements.add(2) = datum3;
    *(*list).elements.add(3) = datum4;
    check_list_invariants(list);
    list
}

/// `list_make5_impl(t, datum1, datum2, datum3, datum4, datum5)`.
///
/// # Safety
/// See [`list_make1_impl`].
pub unsafe fn list_make5_impl(
    t: NodeTag,
    datum1: ListCell,
    datum2: ListCell,
    datum3: ListCell,
    datum4: ListCell,
    datum5: ListCell,
) -> *mut List {
    let list = new_list(t, 5);

    *(*list).elements.add(0) = datum1;
    *(*list).elements.add(1) = datum2;
    *(*list).elements.add(2) = datum3;
    *(*list).elements.add(3) = datum4;
    *(*list).elements.add(4) = datum5;
    check_list_invariants(list);
    list
}

/*
 * Make room for a new head cell in the given (non-NIL) list.
 *
 * The data in the new head cell is undefined; the caller should be
 * sure to fill it in
 */
unsafe fn new_head_cell(list: *mut List) {
    /* Enlarge array if necessary */
    if (*list).length >= (*list).max_length {
        enlarge_list(list, (*list).length + 1);
    }
    /* Now shove the existing data over */
    core::ptr::copy(
        (*list).elements.add(0),
        (*list).elements.add(1),
        (*list).length as usize,
    );
    (*list).length += 1;
}

/*
 * Make room for a new tail cell in the given (non-NIL) list.
 *
 * The data in the new tail cell is undefined; the caller should be
 * sure to fill it in
 */
unsafe fn new_tail_cell(list: *mut List) {
    /* Enlarge array if necessary */
    if (*list).length >= (*list).max_length {
        enlarge_list(list, (*list).length + 1);
    }
    (*list).length += 1;
}

/*
 * Append a pointer to the list. A pointer to the modified list is
 * returned. Note that this function may or may not destructively
 * modify the list; callers should always use this function's return
 * value, rather than continuing to use the pointer passed as the
 * first argument.
 */
/// # Safety
/// `list` must be NIL or a valid pointer List.
pub unsafe fn lappend(mut list: *mut List, datum: *mut c_void) -> *mut List {
    Assert!(is_pointer_list(list));

    if list == NIL {
        list = new_list(T_List, 1);
    } else {
        new_tail_cell(list);
    }

    *lfirst_mut(list_last_cell(list)) = datum;
    check_list_invariants(list);
    list
}

/*
 * Append an integer to the specified list. See lappend()
 */
/// # Safety
/// `list` must be NIL or a valid IntList.
pub unsafe fn lappend_int(mut list: *mut List, datum: c_int) -> *mut List {
    Assert!(is_integer_list(list));

    if list == NIL {
        list = new_list(T_IntList, 1);
    } else {
        new_tail_cell(list);
    }

    *lfirst_int_mut(list_last_cell(list)) = datum;
    check_list_invariants(list);
    list
}

/*
 * Append an OID to the specified list. See lappend()
 */
/// # Safety
/// `list` must be NIL or a valid OidList.
pub unsafe fn lappend_oid(mut list: *mut List, datum: Oid) -> *mut List {
    Assert!(is_oid_list(list));

    if list == NIL {
        list = new_list(T_OidList, 1);
    } else {
        new_tail_cell(list);
    }

    *lfirst_oid_mut(list_last_cell(list)) = datum;
    check_list_invariants(list);
    list
}

/*
 * Append a TransactionId to the specified list. See lappend()
 */
/// # Safety
/// `list` must be NIL or a valid XidList.
pub unsafe fn lappend_xid(mut list: *mut List, datum: TransactionId) -> *mut List {
    Assert!(is_xid_list(list));

    if list == NIL {
        list = new_list(T_XidList, 1);
    } else {
        new_tail_cell(list);
    }

    *lfirst_xid_mut(list_last_cell(list)) = datum;
    check_list_invariants(list);
    list
}

/*
 * Make room for a new cell at position 'pos' (measured from 0).
 * The data in the cell is left undefined, and must be filled in by the
 * caller. 'list' is assumed to be non-NIL, and 'pos' must be a valid
 * list position, ie, 0 <= pos <= list's length.
 * Returns address of the new cell.
 */
unsafe fn insert_new_cell(list: *mut List, pos: c_int) -> *mut ListCell {
    Assert!(pos >= 0 && pos <= (*list).length);

    /* Enlarge array if necessary */
    if (*list).length >= (*list).max_length {
        enlarge_list(list, (*list).length + 1);
    }
    /* Now shove the existing data over */
    if pos < (*list).length {
        core::ptr::copy(
            (*list).elements.add(pos as usize),
            (*list).elements.add((pos + 1) as usize),
            ((*list).length - pos) as usize,
        );
    }
    (*list).length += 1;

    (*list).elements.add(pos as usize)
}

/*
 * Insert the given datum at position 'pos' (measured from 0) in the list.
 * 'pos' must be valid, ie, 0 <= pos <= list's length.
 *
 * Note that this takes time proportional to the distance to the end of the
 * list, since the following entries must be moved.
 */
/// # Safety
/// `list` must be NIL or a valid pointer List; `pos` valid.
pub unsafe fn list_insert_nth(list: *mut List, pos: c_int, datum: *mut c_void) -> *mut List {
    if list == NIL {
        Assert!(pos == 0);
        return list_make1!(datum);
    }
    Assert!(is_pointer_list(list));
    *lfirst_mut(insert_new_cell(list, pos)) = datum;
    check_list_invariants(list);
    list
}

/// # Safety
/// `list` must be NIL or a valid IntList; `pos` valid.
pub unsafe fn list_insert_nth_int(list: *mut List, pos: c_int, datum: c_int) -> *mut List {
    if list == NIL {
        Assert!(pos == 0);
        return list_make1_int!(datum);
    }
    Assert!(is_integer_list(list));
    *lfirst_int_mut(insert_new_cell(list, pos)) = datum;
    check_list_invariants(list);
    list
}

/// # Safety
/// `list` must be NIL or a valid OidList; `pos` valid.
pub unsafe fn list_insert_nth_oid(list: *mut List, pos: c_int, datum: Oid) -> *mut List {
    if list == NIL {
        Assert!(pos == 0);
        return list_make1_oid!(datum);
    }
    Assert!(is_oid_list(list));
    *lfirst_oid_mut(insert_new_cell(list, pos)) = datum;
    check_list_invariants(list);
    list
}

/*
 * Prepend a new element to the list. A pointer to the modified list
 * is returned. Note that this function may or may not destructively
 * modify the list; callers should always use this function's return
 * value, rather than continuing to use the pointer passed as the
 * second argument.
 *
 * Note that this takes time proportional to the length of the list,
 * since the existing entries must be moved.
 *
 * Caution: before Postgres 8.0, the original List was unmodified and
 * could be considered to retain its separate identity.  This is no longer
 * the case.
 */
/// # Safety
/// `list` must be NIL or a valid pointer List.
pub unsafe fn lcons(datum: *mut c_void, mut list: *mut List) -> *mut List {
    Assert!(is_pointer_list(list));

    if list == NIL {
        list = new_list(T_List, 1);
    } else {
        new_head_cell(list);
    }

    *lfirst_mut(list_nth_cell(list, 0)) = datum;
    check_list_invariants(list);
    list
}

/*
 * Prepend an integer to the list. See lcons()
 */
/// # Safety
/// `list` must be NIL or a valid IntList.
pub unsafe fn lcons_int(datum: c_int, mut list: *mut List) -> *mut List {
    Assert!(is_integer_list(list));

    if list == NIL {
        list = new_list(T_IntList, 1);
    } else {
        new_head_cell(list);
    }

    *lfirst_int_mut(list_nth_cell(list, 0)) = datum;
    check_list_invariants(list);
    list
}

/*
 * Prepend an OID to the list. See lcons()
 */
/// # Safety
/// `list` must be NIL or a valid OidList.
pub unsafe fn lcons_oid(datum: Oid, mut list: *mut List) -> *mut List {
    Assert!(is_oid_list(list));

    if list == NIL {
        list = new_list(T_OidList, 1);
    } else {
        new_head_cell(list);
    }

    *lfirst_oid_mut(list_nth_cell(list, 0)) = datum;
    check_list_invariants(list);
    list
}

/*
 * Concatenate list2 to the end of list1, and return list1.
 *
 * This is equivalent to lappend'ing each element of list2, in order, to list1.
 * list1 is destructively changed, list2 is not.  (However, in the case of
 * pointer lists, list1 and list2 will point to the same structures.)
 *
 * Callers should be sure to use the return value as the new pointer to the
 * concatenated list: the 'list1' input pointer may or may not be the same
 * as the returned pointer.
 *
 * Note that this takes at least time proportional to the length of list2.
 * It'd typically be the case that we have to enlarge list1's storage,
 * probably adding time proportional to the length of list1.
 */
/// # Safety
/// Both lists must be NIL or valid Lists of the same type.
pub unsafe fn list_concat(list1: *mut List, list2: *const List) -> *mut List {
    let new_len: c_int;

    if list1 == NIL {
        return list_copy(list2);
    }
    if list2 == NIL {
        return list1;
    }

    Assert!((*list1).r#type == (*list2).r#type);

    new_len = (*list1).length + (*list2).length;
    /* Enlarge array if necessary */
    if new_len > (*list1).max_length {
        enlarge_list(list1, new_len);
    }

    /* Even if list1 == list2, using memcpy should be safe here */
    core::ptr::copy_nonoverlapping(
        (*list2).elements.add(0),
        (*list1).elements.add((*list1).length as usize),
        (*list2).length as usize,
    );
    (*list1).length = new_len;

    check_list_invariants(list1);
    list1
}

/*
 * Form a new list by concatenating the elements of list1 and list2.
 *
 * Neither input list is modified.  (However, if they are pointer lists,
 * the output list will point to the same structures.)
 *
 * This is equivalent to, but more efficient than,
 * list_concat(list_copy(list1), list2).
 * Note that some pre-v13 code might list_copy list2 as well, but that's
 * pointless now.
 */
/// # Safety
/// Both lists must be NIL or valid Lists of the same type.
pub unsafe fn list_concat_copy(list1: *const List, list2: *const List) -> *mut List {
    let result: *mut List;
    let new_len: c_int;

    if list1 == NIL {
        return list_copy(list2);
    }
    if list2 == NIL {
        return list_copy(list1);
    }

    Assert!((*list1).r#type == (*list2).r#type);

    new_len = (*list1).length + (*list2).length;
    result = new_list((*list1).r#type, new_len);
    core::ptr::copy_nonoverlapping(
        (*list1).elements,
        (*result).elements,
        (*list1).length as usize,
    );
    core::ptr::copy_nonoverlapping(
        (*list2).elements,
        (*result).elements.add((*list1).length as usize),
        (*list2).length as usize,
    );

    check_list_invariants(result);
    result
}

/*
 * Truncate 'list' to contain no more than 'new_size' elements. This
 * modifies the list in-place! Despite this, callers should use the
 * pointer returned by this function to refer to the newly truncated
 * list -- it may or may not be the same as the pointer that was
 * passed.
 *
 * Note that any cells removed by list_truncate() are NOT pfree'd.
 */
/// # Safety
/// `list` must be NIL or a valid List.
pub unsafe fn list_truncate(list: *mut List, new_size: c_int) -> *mut List {
    if new_size <= 0 {
        return NIL; /* truncate to zero length */
    }

    /* If asked to effectively extend the list, do nothing */
    if new_size < list_length(list) {
        (*list).length = new_size;
    }

    /*
     * Note: unlike the individual-list-cell deletion functions, we don't move
     * the list cells to new storage, even in DEBUG_LIST_MEMORY_USAGE mode.
     * This is because none of them can move in this operation, so just like
     * in the old cons-cell-based implementation, this function doesn't
     * invalidate any pointers to cells of the list.  This is also the reason
     * for not wiping the memory of the deleted cells: the old code didn't
     * free them either.  Perhaps later we'll tighten this up.
     */

    list
}

/*
 * Return true iff 'datum' is a member of the list. Equality is
 * determined via equal(), so callers should ensure that they pass a
 * Node as 'datum'.
 *
 * This does a simple linear search --- avoid using it on long lists.
 */
/// # Safety
/// `list` must be NIL or a valid pointer List; cells must hold Node pointers.
pub unsafe fn list_member(list: *const List, datum: *const c_void) -> bool {
    Assert!(is_pointer_list(list));
    check_list_invariants(list);

    foreach!(cell, list, {
        if equal(lfirst(current_cell!(cell)) as *const c_void, datum) {
            return true;
        }
    });

    false
}

/*
 * Return true iff 'datum' is a member of the list. Equality is
 * determined by using simple pointer comparison.
 */
/// # Safety
/// `list` must be NIL or a valid pointer List.
pub unsafe fn list_member_ptr(list: *const List, datum: *const c_void) -> bool {
    Assert!(is_pointer_list(list));
    check_list_invariants(list);

    foreach!(cell, list, {
        if lfirst(current_cell!(cell)) as *const c_void == datum {
            return true;
        }
    });

    false
}

/*
 * Return true iff the integer 'datum' is a member of the list.
 */
/// # Safety
/// `list` must be NIL or a valid IntList.
pub unsafe fn list_member_int(list: *const List, datum: c_int) -> bool {
    Assert!(is_integer_list(list));
    check_list_invariants(list);

    foreach!(cell, list, {
        if lfirst_int(current_cell!(cell)) == datum {
            return true;
        }
    });

    false
}

/*
 * Return true iff the OID 'datum' is a member of the list.
 */
/// # Safety
/// `list` must be NIL or a valid OidList.
pub unsafe fn list_member_oid(list: *const List, datum: Oid) -> bool {
    Assert!(is_oid_list(list));
    check_list_invariants(list);

    foreach!(cell, list, {
        if lfirst_oid(current_cell!(cell)) == datum {
            return true;
        }
    });

    false
}

/*
 * Return true iff the TransactionId 'datum' is a member of the list.
 */
/// # Safety
/// `list` must be NIL or a valid XidList.
pub unsafe fn list_member_xid(list: *const List, datum: TransactionId) -> bool {
    Assert!(is_xid_list(list));
    check_list_invariants(list);

    foreach!(cell, list, {
        if lfirst_xid(current_cell!(cell)) == datum {
            return true;
        }
    });

    false
}

/*
 * Delete the n'th cell (counting from 0) in list.
 *
 * The List is pfree'd if this was the last member.
 *
 * Note that this takes time proportional to the distance to the end of the
 * list, since the following entries must be moved.
 */
/// # Safety
/// `list` must be non-NIL and `n` in range.
pub unsafe fn list_delete_nth_cell(list: *mut List, n: c_int) -> *mut List {
    check_list_invariants(list);

    Assert!(n >= 0 && n < (*list).length);

    /*
     * If we're about to delete the last node from the list, free the whole
     * list instead and return NIL, which is the only valid representation of
     * a zero-length list.
     */
    if (*list).length == 1 {
        list_free(list);
        return NIL;
    }

    /*
     * Otherwise, we normally just collapse out the removed element.  But for
     * debugging purposes, move the whole list contents someplace else.
     * (Only the default path is translated.)
     *
     * (Note that we *must* keep the contents in the same memory context.)
     */
    core::ptr::copy(
        (*list).elements.add((n + 1) as usize),
        (*list).elements.add(n as usize),
        ((*list).length - 1 - n) as usize,
    );
    (*list).length -= 1;

    list
}

/*
 * Delete 'cell' from 'list'.
 *
 * The List is pfree'd if this was the last member.  However, we do not
 * touch any data the cell might've been pointing to.
 *
 * Note that this takes time proportional to the distance to the end of the
 * list, since the following entries must be moved.
 */
/// # Safety
/// `cell` must point within `list`'s element array.
pub unsafe fn list_delete_cell(list: *mut List, cell: *mut ListCell) -> *mut List {
    list_delete_nth_cell(list, cell.offset_from((*list).elements) as c_int)
}

/*
 * Delete the first cell in list that matches datum, if any.
 * Equality is determined via equal().
 *
 * This does a simple linear search --- avoid using it on long lists.
 */
/// # Safety
/// `list` must be NIL or a valid pointer List.
pub unsafe fn list_delete(list: *mut List, datum: *mut c_void) -> *mut List {
    Assert!(is_pointer_list(list));
    check_list_invariants(list);

    foreach!(cell, list, {
        if equal(lfirst(current_cell!(cell)) as *const c_void, datum as *const c_void) {
            return list_delete_cell(list, current_cell!(cell));
        }
    });

    /* Didn't find a match: return the list unmodified */
    list
}

/// As above, but use simple pointer equality.
///
/// # Safety
/// `list` must be NIL or a valid pointer List.
pub unsafe fn list_delete_ptr(list: *mut List, datum: *mut c_void) -> *mut List {
    Assert!(is_pointer_list(list));
    check_list_invariants(list);

    foreach!(cell, list, {
        if lfirst(current_cell!(cell)) == datum {
            return list_delete_cell(list, current_cell!(cell));
        }
    });

    /* Didn't find a match: return the list unmodified */
    list
}

/// As above, but for integers.
///
/// # Safety
/// `list` must be NIL or a valid IntList.
pub unsafe fn list_delete_int(list: *mut List, datum: c_int) -> *mut List {
    Assert!(is_integer_list(list));
    check_list_invariants(list);

    foreach!(cell, list, {
        if lfirst_int(current_cell!(cell)) == datum {
            return list_delete_cell(list, current_cell!(cell));
        }
    });

    /* Didn't find a match: return the list unmodified */
    list
}

/// As above, but for OIDs.
///
/// # Safety
/// `list` must be NIL or a valid OidList.
pub unsafe fn list_delete_oid(list: *mut List, datum: Oid) -> *mut List {
    Assert!(is_oid_list(list));
    check_list_invariants(list);

    foreach!(cell, list, {
        if lfirst_oid(current_cell!(cell)) == datum {
            return list_delete_cell(list, current_cell!(cell));
        }
    });

    /* Didn't find a match: return the list unmodified */
    list
}

/*
 * Delete the first element of the list.
 *
 * This is useful to replace the Lisp-y code "list = lnext(list);" in cases
 * where the intent is to alter the list rather than just traverse it.
 * Beware that the list is modified, whereas the Lisp-y coding leaves
 * the original list head intact in case there's another pointer to it.
 *
 * Note that this takes time proportional to the length of the list,
 * since the remaining entries must be moved.  Consider reversing the
 * list order so that you can use list_delete_last() instead.  However,
 * if that causes you to replace lappend() with lcons(), you haven't
 * improved matters.  (In short, you can make an efficient stack from
 * a List, but not an efficient FIFO queue.)
 */
/// # Safety
/// `list` must be NIL or a valid List.
pub unsafe fn list_delete_first(list: *mut List) -> *mut List {
    check_list_invariants(list);

    if list == NIL {
        return NIL; /* would an error be better? */
    }

    list_delete_nth_cell(list, 0)
}

/*
 * Delete the last element of the list.
 */
/// # Safety
/// `list` must be NIL or a valid List.
pub unsafe fn list_delete_last(list: *mut List) -> *mut List {
    check_list_invariants(list);

    if list == NIL {
        return NIL; /* would an error be better? */
    }

    /* list_truncate won't free list if it goes to empty, but this should */
    if list_length(list) <= 1 {
        list_free(list);
        return NIL;
    }

    list_truncate(list, list_length(list) - 1)
}

/*
 * Delete the first N cells of the list.
 *
 * The List is pfree'd if the request causes all cells to be deleted.
 *
 * Note that this takes time proportional to the distance to the end of the
 * list, since the following entries must be moved.
 */
/// # Safety
/// `list` must be NIL or a valid List.
pub unsafe fn list_delete_first_n(list: *mut List, n: c_int) -> *mut List {
    check_list_invariants(list);

    /* No-op request? */
    if n <= 0 {
        return list;
    }

    /* Delete whole list? */
    if n >= list_length(list) {
        list_free(list);
        return NIL;
    }

    /*
     * Otherwise, we normally just collapse out the removed elements.  But for
     * debugging purposes, move the whole list contents someplace else.
     * (Only the default path is translated.)
     *
     * (Note that we *must* keep the contents in the same memory context.)
     */
    core::ptr::copy(
        (*list).elements.add(n as usize),
        (*list).elements.add(0),
        ((*list).length - n) as usize,
    );
    (*list).length -= n;

    list
}

/*
 * Generate the union of two lists. This is calculated by copying
 * list1 via list_copy(), then adding to it all the members of list2
 * that aren't already in list1.
 *
 * Whether an element is already a member of the list is determined
 * via equal().
 *
 * The returned list is newly-allocated, although the content of the
 * cells is the same (i.e. any pointed-to objects are not copied).
 *
 * NB: this function will NOT remove any duplicates that are present
 * in list1 (so it only performs a "union" if list1 is known unique to
 * start with).  Also, if you are about to write "x = list_union(x, y)"
 * you probably want to use list_concat_unique() instead to avoid wasting
 * the storage of the old x list.
 *
 * Note that this takes time proportional to the product of the list
 * lengths, so beware of using it on long lists.
 */
/// # Safety
/// Both lists must be NIL or valid pointer Lists.
pub unsafe fn list_union(list1: *const List, list2: *const List) -> *mut List {
    let mut result: *mut List;

    Assert!(is_pointer_list(list1));
    Assert!(is_pointer_list(list2));

    result = list_copy(list1);
    foreach!(cell, list2, {
        if !list_member(result, lfirst(current_cell!(cell)) as *const c_void) {
            result = lappend(result, lfirst(current_cell!(cell)));
        }
    });

    check_list_invariants(result);
    result
}

/*
 * This variant of list_union() determines duplicates via simple
 * pointer comparison.
 */
/// # Safety
/// Both lists must be NIL or valid pointer Lists.
pub unsafe fn list_union_ptr(list1: *const List, list2: *const List) -> *mut List {
    let mut result: *mut List;

    Assert!(is_pointer_list(list1));
    Assert!(is_pointer_list(list2));

    result = list_copy(list1);
    foreach!(cell, list2, {
        if !list_member_ptr(result, lfirst(current_cell!(cell)) as *const c_void) {
            result = lappend(result, lfirst(current_cell!(cell)));
        }
    });

    check_list_invariants(result);
    result
}

/*
 * This variant of list_union() operates upon lists of integers.
 */
/// # Safety
/// Both lists must be NIL or valid IntLists.
pub unsafe fn list_union_int(list1: *const List, list2: *const List) -> *mut List {
    let mut result: *mut List;

    Assert!(is_integer_list(list1));
    Assert!(is_integer_list(list2));

    result = list_copy(list1);
    foreach!(cell, list2, {
        if !list_member_int(result, lfirst_int(current_cell!(cell))) {
            result = lappend_int(result, lfirst_int(current_cell!(cell)));
        }
    });

    check_list_invariants(result);
    result
}

/*
 * This variant of list_union() operates upon lists of OIDs.
 */
/// # Safety
/// Both lists must be NIL or valid OidLists.
pub unsafe fn list_union_oid(list1: *const List, list2: *const List) -> *mut List {
    let mut result: *mut List;

    Assert!(is_oid_list(list1));
    Assert!(is_oid_list(list2));

    result = list_copy(list1);
    foreach!(cell, list2, {
        if !list_member_oid(result, lfirst_oid(current_cell!(cell))) {
            result = lappend_oid(result, lfirst_oid(current_cell!(cell)));
        }
    });

    check_list_invariants(result);
    result
}

/*
 * Return a list that contains all the cells that are in both list1 and
 * list2.  The returned list is freshly allocated via palloc(), but the
 * cells themselves point to the same objects as the cells of the
 * input lists.
 *
 * Duplicate entries in list1 will not be suppressed, so it's only a true
 * "intersection" if list1 is known unique beforehand.
 *
 * This variant works on lists of pointers, and determines list
 * membership via equal().  Note that the list1 member will be pointed
 * to in the result.
 *
 * Note that this takes time proportional to the product of the list
 * lengths, so beware of using it on long lists.
 */
/// # Safety
/// Both lists must be NIL or valid pointer Lists.
pub unsafe fn list_intersection(list1: *const List, list2: *const List) -> *mut List {
    let mut result: *mut List;

    if list1 == NIL || list2 == NIL {
        return NIL;
    }

    Assert!(is_pointer_list(list1));
    Assert!(is_pointer_list(list2));

    result = NIL;
    foreach!(cell, list1, {
        if list_member(list2, lfirst(current_cell!(cell)) as *const c_void) {
            result = lappend(result, lfirst(current_cell!(cell)));
        }
    });

    check_list_invariants(result);
    result
}

/*
 * As list_intersection but operates on lists of integers.
 */
/// # Safety
/// Both lists must be NIL or valid IntLists.
pub unsafe fn list_intersection_int(list1: *const List, list2: *const List) -> *mut List {
    let mut result: *mut List;

    if list1 == NIL || list2 == NIL {
        return NIL;
    }

    Assert!(is_integer_list(list1));
    Assert!(is_integer_list(list2));

    result = NIL;
    foreach!(cell, list1, {
        if list_member_int(list2, lfirst_int(current_cell!(cell))) {
            result = lappend_int(result, lfirst_int(current_cell!(cell)));
        }
    });

    check_list_invariants(result);
    result
}

/*
 * Return a list that contains all the cells in list1 that are not in
 * list2. The returned list is freshly allocated via palloc(), but the
 * cells themselves point to the same objects as the cells of the
 * input lists.
 *
 * This variant works on lists of pointers, and determines list
 * membership via equal()
 *
 * Note that this takes time proportional to the product of the list
 * lengths, so beware of using it on long lists.
 */
/// # Safety
/// Both lists must be NIL or valid pointer Lists.
pub unsafe fn list_difference(list1: *const List, list2: *const List) -> *mut List {
    let mut result: *mut List = NIL;

    Assert!(is_pointer_list(list1));
    Assert!(is_pointer_list(list2));

    if list2 == NIL {
        return list_copy(list1);
    }

    foreach!(cell, list1, {
        if !list_member(list2, lfirst(current_cell!(cell)) as *const c_void) {
            result = lappend(result, lfirst(current_cell!(cell)));
        }
    });

    check_list_invariants(result);
    result
}

/*
 * This variant of list_difference() determines list membership via
 * simple pointer equality.
 */
/// # Safety
/// Both lists must be NIL or valid pointer Lists.
pub unsafe fn list_difference_ptr(list1: *const List, list2: *const List) -> *mut List {
    let mut result: *mut List = NIL;

    Assert!(is_pointer_list(list1));
    Assert!(is_pointer_list(list2));

    if list2 == NIL {
        return list_copy(list1);
    }

    foreach!(cell, list1, {
        if !list_member_ptr(list2, lfirst(current_cell!(cell)) as *const c_void) {
            result = lappend(result, lfirst(current_cell!(cell)));
        }
    });

    check_list_invariants(result);
    result
}

/*
 * This variant of list_difference() operates upon lists of integers.
 */
/// # Safety
/// Both lists must be NIL or valid IntLists.
pub unsafe fn list_difference_int(list1: *const List, list2: *const List) -> *mut List {
    let mut result: *mut List = NIL;

    Assert!(is_integer_list(list1));
    Assert!(is_integer_list(list2));

    if list2 == NIL {
        return list_copy(list1);
    }

    foreach!(cell, list1, {
        if !list_member_int(list2, lfirst_int(current_cell!(cell))) {
            result = lappend_int(result, lfirst_int(current_cell!(cell)));
        }
    });

    check_list_invariants(result);
    result
}

/*
 * This variant of list_difference() operates upon lists of OIDs.
 */
/// # Safety
/// Both lists must be NIL or valid OidLists.
pub unsafe fn list_difference_oid(list1: *const List, list2: *const List) -> *mut List {
    let mut result: *mut List = NIL;

    Assert!(is_oid_list(list1));
    Assert!(is_oid_list(list2));

    if list2 == NIL {
        return list_copy(list1);
    }

    foreach!(cell, list1, {
        if !list_member_oid(list2, lfirst_oid(current_cell!(cell))) {
            result = lappend_oid(result, lfirst_oid(current_cell!(cell)));
        }
    });

    check_list_invariants(result);
    result
}

/*
 * Append datum to list, but only if it isn't already in the list.
 *
 * Whether an element is already a member of the list is determined
 * via equal().
 *
 * This does a simple linear search --- avoid using it on long lists.
 */
/// # Safety
/// `list` must be NIL or a valid pointer List.
pub unsafe fn list_append_unique(list: *mut List, datum: *mut c_void) -> *mut List {
    if list_member(list, datum) {
        list
    } else {
        lappend(list, datum)
    }
}

/*
 * This variant of list_append_unique() determines list membership via
 * simple pointer equality.
 */
/// # Safety
/// `list` must be NIL or a valid pointer List.
pub unsafe fn list_append_unique_ptr(list: *mut List, datum: *mut c_void) -> *mut List {
    if list_member_ptr(list, datum) {
        list
    } else {
        lappend(list, datum)
    }
}

/*
 * This variant of list_append_unique() operates upon lists of integers.
 */
/// # Safety
/// `list` must be NIL or a valid IntList.
pub unsafe fn list_append_unique_int(list: *mut List, datum: c_int) -> *mut List {
    if list_member_int(list, datum) {
        list
    } else {
        lappend_int(list, datum)
    }
}

/*
 * This variant of list_append_unique() operates upon lists of OIDs.
 */
/// # Safety
/// `list` must be NIL or a valid OidList.
pub unsafe fn list_append_unique_oid(list: *mut List, datum: Oid) -> *mut List {
    if list_member_oid(list, datum) {
        list
    } else {
        lappend_oid(list, datum)
    }
}

/*
 * Append to list1 each member of list2 that isn't already in list1.
 *
 * Whether an element is already a member of the list is determined
 * via equal().
 *
 * This is almost the same functionality as list_union(), but list1 is
 * modified in-place rather than being copied. However, callers of this
 * function may have strict ordering expectations -- i.e. that the relative
 * order of those list2 elements that are not duplicates is preserved.
 *
 * Note that this takes time proportional to the product of the list
 * lengths, so beware of using it on long lists.
 */
/// # Safety
/// Both lists must be NIL or valid pointer Lists.
pub unsafe fn list_concat_unique(mut list1: *mut List, list2: *const List) -> *mut List {
    Assert!(is_pointer_list(list1));
    Assert!(is_pointer_list(list2));

    foreach!(cell, list2, {
        if !list_member(list1, lfirst(current_cell!(cell)) as *const c_void) {
            list1 = lappend(list1, lfirst(current_cell!(cell)));
        }
    });

    check_list_invariants(list1);
    list1
}

/*
 * This variant of list_concat_unique() determines list membership via
 * simple pointer equality.
 */
/// # Safety
/// Both lists must be NIL or valid pointer Lists.
pub unsafe fn list_concat_unique_ptr(mut list1: *mut List, list2: *const List) -> *mut List {
    Assert!(is_pointer_list(list1));
    Assert!(is_pointer_list(list2));

    foreach!(cell, list2, {
        if !list_member_ptr(list1, lfirst(current_cell!(cell)) as *const c_void) {
            list1 = lappend(list1, lfirst(current_cell!(cell)));
        }
    });

    check_list_invariants(list1);
    list1
}

/*
 * This variant of list_concat_unique() operates upon lists of integers.
 */
/// # Safety
/// Both lists must be NIL or valid IntLists.
pub unsafe fn list_concat_unique_int(mut list1: *mut List, list2: *const List) -> *mut List {
    Assert!(is_integer_list(list1));
    Assert!(is_integer_list(list2));

    foreach!(cell, list2, {
        if !list_member_int(list1, lfirst_int(current_cell!(cell))) {
            list1 = lappend_int(list1, lfirst_int(current_cell!(cell)));
        }
    });

    check_list_invariants(list1);
    list1
}

/*
 * This variant of list_concat_unique() operates upon lists of OIDs.
 */
/// # Safety
/// Both lists must be NIL or valid OidLists.
pub unsafe fn list_concat_unique_oid(mut list1: *mut List, list2: *const List) -> *mut List {
    Assert!(is_oid_list(list1));
    Assert!(is_oid_list(list2));

    foreach!(cell, list2, {
        if !list_member_oid(list1, lfirst_oid(current_cell!(cell))) {
            list1 = lappend_oid(list1, lfirst_oid(current_cell!(cell)));
        }
    });

    check_list_invariants(list1);
    list1
}

/*
 * Remove adjacent duplicates in a list of OIDs.
 *
 * It is caller's responsibility to have sorted the list to bring duplicates
 * together, perhaps via list_sort(list, list_oid_cmp).
 *
 * Note that this takes time proportional to the length of the list.
 */
/// # Safety
/// `list` must be NIL or a valid OidList.
pub unsafe fn list_deduplicate_oid(list: *mut List) {
    let len: c_int;

    Assert!(is_oid_list(list));
    len = list_length(list);
    if len > 1 {
        let elements = (*list).elements;
        let mut i: c_int = 0;

        let mut j: c_int = 1;
        while j < len {
            if (*elements.add(i as usize)).oid_value != (*elements.add(j as usize)).oid_value {
                i += 1;
                (*elements.add(i as usize)).oid_value = (*elements.add(j as usize)).oid_value;
            }
            j += 1;
        }
        (*list).length = i + 1;
    }
    check_list_invariants(list);
}

/*
 * Free all storage in a list, and optionally the pointed-to elements
 */
unsafe fn list_free_private(list: *mut List, deep: bool) {
    if list == NIL {
        return; /* nothing to do */
    }

    check_list_invariants(list);

    if deep {
        for i in 0..(*list).length {
            pfree(lfirst((*list).elements.add(i as usize)));
        }
    }
    if (*list).elements != initial_elements_ptr(list) {
        pfree((*list).elements as *mut c_void);
    }
    pfree(list as *mut c_void);
}

/*
 * Free all the cells of the list, as well as the list itself. Any
 * objects that are pointed-to by the cells of the list are NOT
 * free'd.
 *
 * On return, the argument to this function has been freed, so the
 * caller would be wise to set it to NIL for safety's sake.
 */
/// # Safety
/// `list` must be NIL or a valid List that is no longer used after this call.
pub unsafe fn list_free(list: *mut List) {
    list_free_private(list, false);
}

/*
 * Free all the cells of the list, the list itself, and all the
 * objects pointed-to by the cells of the list (each element in the
 * list must contain a pointer to a palloc()'d region of memory!)
 *
 * On return, the argument to this function has been freed, so the
 * caller would be wise to set it to NIL for safety's sake.
 */
/// # Safety
/// `list` must be NIL or a valid pointer List whose cells point to palloc'd memory.
pub unsafe fn list_free_deep(list: *mut List) {
    /*
     * A "deep" free operation only makes sense on a list of pointers.
     */
    Assert!(is_pointer_list(list));
    list_free_private(list, true);
}

/*
 * Return a shallow copy of the specified list.
 */
/// # Safety
/// `oldlist` must be NIL or a valid List.
pub unsafe fn list_copy(oldlist: *const List) -> *mut List {
    let newlist: *mut List;

    if oldlist == NIL {
        return NIL;
    }

    newlist = new_list((*oldlist).r#type, (*oldlist).length);
    core::ptr::copy_nonoverlapping(
        (*oldlist).elements,
        (*newlist).elements,
        (*newlist).length as usize,
    );

    check_list_invariants(newlist);
    newlist
}

/*
 * Return a shallow copy of the specified list containing only the first 'len'
 * elements.  If oldlist is shorter than 'len' then we copy the entire list.
 */
/// # Safety
/// `oldlist` must be NIL or a valid List.
pub unsafe fn list_copy_head(oldlist: *const List, mut len: c_int) -> *mut List {
    let newlist: *mut List;

    if oldlist == NIL || len <= 0 {
        return NIL;
    }

    len = Min((*oldlist).length, len);

    newlist = new_list((*oldlist).r#type, len);
    core::ptr::copy_nonoverlapping((*oldlist).elements, (*newlist).elements, len as usize);

    check_list_invariants(newlist);
    newlist
}

/*
 * Return a shallow copy of the specified list, without the first N elements.
 */
/// # Safety
/// `oldlist` must be NIL or a valid List.
pub unsafe fn list_copy_tail(oldlist: *const List, mut nskip: c_int) -> *mut List {
    let newlist: *mut List;

    if nskip < 0 {
        nskip = 0; /* would it be better to elog? */
    }

    if oldlist == NIL || nskip >= (*oldlist).length {
        return NIL;
    }

    newlist = new_list((*oldlist).r#type, (*oldlist).length - nskip);
    core::ptr::copy_nonoverlapping(
        (*oldlist).elements.add(nskip as usize),
        (*newlist).elements,
        (*newlist).length as usize,
    );

    check_list_invariants(newlist);
    newlist
}

/*
 * Return a deep copy of the specified list.
 *
 * The list elements are copied via copyObject(), so that this function's
 * idea of a "deep" copy is considerably deeper than what list_free_deep()
 * means by the same word.
 */
/// # Safety
/// `oldlist` must be NIL or a valid pointer List whose cells are Node pointers.
pub unsafe fn list_copy_deep(oldlist: *const List) -> *mut List {
    let newlist: *mut List;

    if oldlist == NIL {
        return NIL;
    }

    /* This is only sensible for pointer Lists */
    Assert!(IsA!(oldlist, T_List));

    newlist = new_list((*oldlist).r#type, (*oldlist).length);
    for i in 0..(*newlist).length {
        *lfirst_mut((*newlist).elements.add(i as usize)) =
            copyObjectImpl(lfirst((*oldlist).elements.add(i as usize)) as *const c_void);
    }

    check_list_invariants(newlist);
    newlist
}

/*
 * Sort a list according to the specified comparator function.
 *
 * The list is sorted in-place.
 *
 * The comparator function is declared to receive arguments of type
 * const ListCell *; this allows it to use lfirst() and variants
 * without casting its arguments.  Otherwise it behaves the same as
 * the comparator function for standard qsort().
 *
 * Like qsort(), this provides no guarantees about sort stability
 * for equal keys.
 *
 * This is based on qsort(), so it likewise has O(N log N) runtime.
 */
/// # Safety
/// `list` must be NIL or a valid List; `cmp` must be a valid comparator.
pub unsafe fn list_sort(list: *mut List, cmp: list_sort_comparator) {
    let len: c_int;

    check_list_invariants(list);

    /* Nothing to do if there's less than two elements */
    len = list_length(list);
    if len > 1 {
        pg_qsort_listcells((*list).elements, len as usize, cmp);
    }
}

/*
 * list_sort comparator for sorting a list into ascending int order.
 */
/// # Safety
/// `p1`/`p2` must point to valid ListCells holding int values.
pub unsafe fn list_int_cmp(p1: *const ListCell, p2: *const ListCell) -> c_int {
    let v1 = lfirst_int(p1);
    let v2 = lfirst_int(p2);

    pg_cmp_s32(v1, v2)
}

/*
 * list_sort comparator for sorting a list into ascending OID order.
 */
/// # Safety
/// `p1`/`p2` must point to valid ListCells holding Oid values.
pub unsafe fn list_oid_cmp(p1: *const ListCell, p2: *const ListCell) -> c_int {
    let v1 = lfirst_oid(p1);
    let v2 = lfirst_oid(p2);

    pg_cmp_u32(v1, v2)
}

/// Comparator type for [`list_sort`]: receives two `*const ListCell`.
///
/// Mirrors C `typedef int (*list_sort_comparator)(const ListCell *, const ListCell *)`.
pub type list_sort_comparator =
    unsafe fn(a: *const ListCell, b: *const ListCell) -> c_int;

// ---------------------------------------------------------------------------
// Local shims for not-yet-translated dependencies.
// ---------------------------------------------------------------------------

/// `pg_cmp_s32` from common/int.h: three-way compare of signed 32-bit ints.
///
/// TODO(pg-port): replace with the translated common/int.rs when it lands.
#[inline]
fn pg_cmp_s32(a: int32, b: int32) -> c_int {
    (a > b) as c_int - (a < b) as c_int
}

/// `pg_cmp_u32` from common/int.h: three-way compare of unsigned 32-bit ints.
///
/// TODO(pg-port): replace with the translated common/int.rs when it lands.
#[inline]
fn pg_cmp_u32(a: uint32, b: uint32) -> c_int {
    (a > b) as c_int - (a < b) as c_int
}

/// `copyObjectImpl(node)` from nodes/copyfuncs.c: deep-copy an arbitrary Node.
///
/// The full implementation is generated by gen_node_support.pl across every node
/// type and is large; it has not been translated yet. `list_copy_deep` is the
/// only caller within this module.
///
/// TODO(pg-port): translate the generated copyfuncs.c (_copyNNN per type).
///
/// # Safety
/// `from` must be NULL or a valid Node pointer.
unsafe fn copyObjectImpl(from: *const c_void) -> *mut c_void {
    if from.is_null() {
        return core::ptr::null_mut();
    }
    let _ = from as *const Node;
    unimplemented!("copyObjectImpl(): deep node copy not yet translated (copyfuncs.c)");
}

/// A faithful `qsort()` over a raw ListCell array driving a C-style comparator.
///
/// list.c uses libc `qsort()` with a `list_sort_comparator`.  Until a port of the
/// PG qsort lands, we sort the raw cell array in place using the same comparator
/// contract (negative/zero/positive).  `ListCell` is `Copy`, so swaps are cheap.
///
/// TODO(pg-port): replace with a translated pg_qsort / port qsort.
///
/// # Safety
/// `base` must point to `n` valid, writable ListCells.
unsafe fn pg_qsort_listcells(base: *mut ListCell, n: usize, cmp: list_sort_comparator) {
    if n < 2 {
        return;
    }
    // Build a slice of the raw cells and sort with the C comparator.
    let cells = core::slice::from_raw_parts_mut(base, n);
    cells.sort_by(|a, b| {
        let r = cmp(a as *const ListCell, b as *const ListCell);
        r.cmp(&0)
    });
}
