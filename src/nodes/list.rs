//! Translation of postgres/src/backend/nodes/list.c
//!
//! Implementation for the PostgreSQL generic list package.
//!
//! See comments in pg_list.rs (translation of nodes/pg_list.h).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! ---------------------------------------------------------------------------
//! Translation notes (deviations from the C source):
//!
//! * The `List`/`ListCell` types, the `NIL` constant, the inline accessors
//!   (`lfirst`/`lfirst_int`/..., `list_length`, `list_nth_cell`,
//!   `list_last_cell`), the `list_make_*_cell` constructors, the `foreach!`/
//!   `current_cell!` looping macros and the `list_sort_comparator` type all come
//!   from nodes/pg_list.h, translated in `crate::nodes::pg_list`; we import them
//!   here rather than redefine them.
//!
//! * The various `*_node` macros are unused in list.c; nothing extra is imported.
//!
//! * The two conditionally-compiled code paths in list.c (DEBUG_LIST_MEMORY_USAGE
//!   and USE_ASSERT_CHECKING) are translated for the default build: only the
//!   non-debug path is emitted, and `check_list_invariants` is always present but
//!   its body uses `Assert!`, which is a no-op in release builds.
//!
//! * `equal()` (equalfuncs.c) and `copyObjectImpl()` (copyfuncs.c) are external
//!   dependencies; `pg_cmp_s32`/`pg_cmp_u32` come from common/int.h and
//!   `pg_nextpower2_32` from port/pg_bitutils.h.
//!
//! * Functions that dereference raw pointers are `pub unsafe fn`.

use crate::prelude::*;
use crate::IsA;
use crate::nodes::equalfuncs::equal;
use crate::nodes::nodes::NodeTag::{T_IntList, T_List, T_OidList, T_XidList};
use crate::nodes::nodes::{Node, NodeTag};
use crate::nodes::pg_list::{
    list_last_cell, list_length, list_make_int_cell, list_make_oid_cell, list_make_ptr_cell,
    list_nth_cell, list_sort_comparator, lfirst, lfirst_int, lfirst_int_mut, lfirst_mut,
    lfirst_oid, lfirst_oid_mut, lfirst_xid, lfirst_xid_mut, List, ListCell, NIL,
};
use crate::port::pg_bitutils::pg_nextpower2_32;
use crate::utils::palloc::{GetMemoryChunkContext, MemoryContextAlloc};
use crate::{current_cell, foreach};
use core::ffi::{c_int, c_void};

/* Overhead for the fixed part of a List header, measured in ListCells */
// LIST_HEADER_OVERHEAD =
//     ((int) ((offsetof(List, initial_elements) - 1) / sizeof(ListCell) + 1))
#[inline]
const fn list_header_overhead() -> c_int {
    let off = core::mem::offset_of!(List, initial_elements);
    let cell = core::mem::size_of::<ListCell>();
    ((off - 1) / cell + 1) as c_int
}

/*
 * Macros to simplify writing assertions about the type of a list; a
 * NIL list is considered to be an empty list of any type.
 */
#[inline]
unsafe fn IsPointerList(l: *const List) -> bool {
    l == NIL || IsA!(l, T_List)
}
#[inline]
unsafe fn IsIntegerList(l: *const List) -> bool {
    l == NIL || IsA!(l, T_IntList)
}
#[inline]
unsafe fn IsOidList(l: *const List) -> bool {
    l == NIL || IsA!(l, T_OidList)
}
#[inline]
unsafe fn IsXidList(l: *const List) -> bool {
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
/// that follow the header within the same palloc chunk.  This mirrors taking the
/// address of the C flexible array member `list->initial_elements`.
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
         * (wipe_mem includes VALGRIND_MAKE_MEM_NOACCESS.)
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
/// # Safety
/// Allocates a List; the resulting pointer must be managed like any List.
pub unsafe fn list_make1_impl(t: NodeTag, datum1: ListCell) -> *mut List {
    let list = new_list(t, 1);

    *(*list).elements.add(0) = datum1;
    check_list_invariants(list);
    list
}

/// # Safety
/// See [`list_make1_impl`].
pub unsafe fn list_make2_impl(t: NodeTag, datum1: ListCell, datum2: ListCell) -> *mut List {
    let list = new_list(t, 2);

    *(*list).elements.add(0) = datum1;
    *(*list).elements.add(1) = datum2;
    check_list_invariants(list);
    list
}

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
    Assert!(IsPointerList(list));

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
    Assert!(IsIntegerList(list));

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
    Assert!(IsOidList(list));

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
    Assert!(IsXidList(list));

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
        return list_make1_impl(T_List, list_make_ptr_cell(datum));
    }
    Assert!(IsPointerList(list));
    *lfirst_mut(insert_new_cell(list, pos)) = datum;
    check_list_invariants(list);
    list
}

/// # Safety
/// `list` must be NIL or a valid IntList; `pos` valid.
pub unsafe fn list_insert_nth_int(list: *mut List, pos: c_int, datum: c_int) -> *mut List {
    if list == NIL {
        Assert!(pos == 0);
        return list_make1_impl(T_IntList, list_make_int_cell(datum));
    }
    Assert!(IsIntegerList(list));
    *lfirst_int_mut(insert_new_cell(list, pos)) = datum;
    check_list_invariants(list);
    list
}

/// # Safety
/// `list` must be NIL or a valid OidList; `pos` valid.
pub unsafe fn list_insert_nth_oid(list: *mut List, pos: c_int, datum: Oid) -> *mut List {
    if list == NIL {
        Assert!(pos == 0);
        return list_make1_impl(T_OidList, list_make_oid_cell(datum));
    }
    Assert!(IsOidList(list));
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
    Assert!(IsPointerList(list));

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
    Assert!(IsIntegerList(list));

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
    Assert!(IsOidList(list));

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
    Assert!(IsPointerList(list));
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
    Assert!(IsPointerList(list));
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
    Assert!(IsIntegerList(list));
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
    Assert!(IsOidList(list));
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
    Assert!(IsXidList(list));
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
    Assert!(IsPointerList(list));
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
    Assert!(IsPointerList(list));
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
    Assert!(IsIntegerList(list));
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
    Assert!(IsOidList(list));
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
 * lengths, so beware of using it on long lists.  (We could probably
 * improve that, but really you should be using some other data structure
 * if this'd be a performance bottleneck.)
 */
/// # Safety
/// Both lists must be NIL or valid pointer Lists.
pub unsafe fn list_union(list1: *const List, list2: *const List) -> *mut List {
    let mut result: *mut List;

    Assert!(IsPointerList(list1));
    Assert!(IsPointerList(list2));

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

    Assert!(IsPointerList(list1));
    Assert!(IsPointerList(list2));

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

    Assert!(IsIntegerList(list1));
    Assert!(IsIntegerList(list2));

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

    Assert!(IsOidList(list1));
    Assert!(IsOidList(list2));

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
 * lengths, so beware of using it on long lists.  (We could probably
 * improve that, but really you should be using some other data structure
 * if this'd be a performance bottleneck.)
 */
/// # Safety
/// Both lists must be NIL or valid pointer Lists.
pub unsafe fn list_intersection(list1: *const List, list2: *const List) -> *mut List {
    let mut result: *mut List;

    if list1 == NIL || list2 == NIL {
        return NIL;
    }

    Assert!(IsPointerList(list1));
    Assert!(IsPointerList(list2));

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

    Assert!(IsIntegerList(list1));
    Assert!(IsIntegerList(list2));

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
 * lengths, so beware of using it on long lists.  (We could probably
 * improve that, but really you should be using some other data structure
 * if this'd be a performance bottleneck.)
 */
/// # Safety
/// Both lists must be NIL or valid pointer Lists.
pub unsafe fn list_difference(list1: *const List, list2: *const List) -> *mut List {
    let mut result: *mut List = NIL;

    Assert!(IsPointerList(list1));
    Assert!(IsPointerList(list2));

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

    Assert!(IsPointerList(list1));
    Assert!(IsPointerList(list2));

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

    Assert!(IsIntegerList(list1));
    Assert!(IsIntegerList(list2));

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

    Assert!(IsOidList(list1));
    Assert!(IsOidList(list2));

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
 * lengths, so beware of using it on long lists.  (We could probably
 * improve that, but really you should be using some other data structure
 * if this'd be a performance bottleneck.)
 */
/// # Safety
/// Both lists must be NIL or valid pointer Lists.
pub unsafe fn list_concat_unique(mut list1: *mut List, list2: *const List) -> *mut List {
    Assert!(IsPointerList(list1));
    Assert!(IsPointerList(list2));

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
    Assert!(IsPointerList(list1));
    Assert!(IsPointerList(list2));

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
    Assert!(IsIntegerList(list1));
    Assert!(IsIntegerList(list2));

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
    Assert!(IsOidList(list1));
    Assert!(IsOidList(list2));

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

    Assert!(IsOidList(list));
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
    Assert!(IsPointerList(list));
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

// ---------------------------------------------------------------------------
// Local shims for not-yet-translated dependencies (functions in other .c files).
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
/// type and is large; `list_copy_deep` is the only caller within this module.
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
    let cells = core::slice::from_raw_parts_mut(base, n);
    cells.sort_by(|a, b| {
        let r = cmp(a as *const ListCell, b as *const ListCell);
        r.cmp(&0)
    });
}
