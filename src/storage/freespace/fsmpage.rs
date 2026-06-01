//! Translation of postgres/src/backend/storage/freespace/fsmpage.c
//! (merged with the struct/macro layer of postgres/src/include/storage/fsm_internals.h:
//!  the FSMPageData struct, the FSMPage typedef, and the
//!  NodesPerPage / NonLeafNodesPerPage / LeafNodesPerPage / SlotsPerFSMPage consts).
//!
//! Routines to search and manipulate one Free-Space-Map page.  An FSM page holds
//! a complete binary tree of free-space "categories" (one uint8 per node) packed
//! into the page's contents area.  The first NonLeafNodesPerPage entries of
//! fp_nodes[] are the upper (interior) nodes; the next LeafNodesPerPage entries
//! are the leaves, one per "slot".  Each interior node holds the Max of its two
//! children, so the root (node 0) always holds the maximum free space available
//! anywhere on the page.  This lets freespace.c treat each page as a black box of
//! SlotsPerFSMPage slots and do get/set/search in O(log N).
//!
//! The fsm_space_avail_to_cat / fsm_space_cat_to_avail category mapping lives in
//! freespace.c in upstream, but is included here per the porting task so the page
//! logic is self-contained and testable; the FSM_CATEGORIES / FSM_CAT_STEP /
//! MaxFSMRequestSize definitions are reproduced from freespace.c.
//!
//! #include "postgres.h"
//! #include "storage/bufmgr.h"        -> NOT YET PORTED: BufferGetPage / BufferGetTag /
//!                                       LockBuffer / MarkBufferDirtyHint and the
//!                                       BUFFER_LOCK_* constants are stubbed below.
//! #include "storage/fsm_internals.h" -> merged here (FSMPageData / FSMPage / *PerPage).
//!   storage/buf.h     -> Buffer = c_int (defined locally; matches executor::tuptable).
//!   storage/bufpage.h -> crate::storage::bufpage (Page, PageGetContents, SizeOfPageHeaderData).
//!   common/relpath.h  -> crate::common::relpath (ForkNumber) and
//!                        crate::common::blkreftable (RelFileLocator) for the
//!                        corrupt-page log message.
//!   storage/block.h   -> crate::storage::block (BlockNumber).
//!   access/htup_details.h -> crate::access::htup_details (MaxHeapTupleSize = MaxFSMRequestSize).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;
use crate::access::htup_details::MaxHeapTupleSize;
use crate::c::{uint8, uint16, Max, MAXALIGN, Size};
use crate::pg_config::BLCKSZ;
use crate::storage::block::BlockNumber;
use crate::storage::bufpage::{Page, PageGetContents, SizeOfPageHeaderData};
use core::ffi::c_int;
use core::mem::offset_of;

// ----------------------------------------------------------------------------
//   storage/buf.h + storage/bufmgr.h shims (the buffer manager is not yet ported)
// ----------------------------------------------------------------------------

/* storage/buf.h: a Buffer is an index into the shared buffer pool (or local). */
pub type Buffer = c_int;

/* storage/bufmgr.h BUFFER_LOCK_* modes. */
const BUFFER_LOCK_UNLOCK: c_int = 0;
const BUFFER_LOCK_EXCLUSIVE: c_int = 2;

/*
 * The four bufmgr entry points used only by fsm_search_avail's torn-page
 * recovery path.  These are stubbed until storage/buffer/bufmgr.c is ported.
 *
 * BufferGetPage is the one we genuinely need for the common case; with the
 * buffer manager absent there is no shared-buffer array to dereference, so it
 * is left unimplemented.  Tests exercise the page-level helpers directly via a
 * raw Page and never go through a Buffer.
 */
#[allow(unused_variables)]
unsafe fn BufferGetPage(buf: Buffer) -> Page {
    // TODO: real impl reads BufferDescriptors / BufferBlocks once bufmgr.c is ported.
    unimplemented!("BufferGetPage: storage/buffer/bufmgr.c not yet ported")
}

#[allow(unused_variables)]
unsafe fn BufferGetTag(
    buf: Buffer,
    rlocator: *mut crate::common::blkreftable::RelFileLocator,
    forknum: *mut crate::common::relpath::ForkNumber,
    blknum: *mut BlockNumber,
) {
    // TODO: real impl in storage/buffer/bufmgr.c.
    unimplemented!("BufferGetTag: storage/buffer/bufmgr.c not yet ported")
}

#[allow(unused_variables)]
unsafe fn LockBuffer(buf: Buffer, mode: c_int) {
    // TODO: real impl in storage/buffer/bufmgr.c.
    unimplemented!("LockBuffer: storage/buffer/bufmgr.c not yet ported")
}

#[allow(unused_variables)]
unsafe fn MarkBufferDirtyHint(buf: Buffer, buffer_std: bool) {
    // TODO: real impl in storage/buffer/bufmgr.c.
    unimplemented!("MarkBufferDirtyHint: storage/buffer/bufmgr.c not yet ported")
}

// ----------------------------------------------------------------------------
//   fsm_internals.h: the FSM page structure
// ----------------------------------------------------------------------------

/*
 * Structure of a FSM page. See src/backend/storage/freespace/README for details.
 */
#[repr(C)]
pub struct FSMPageData {
    /*
     * fsm_search_avail() tries to spread the load of multiple backends by
     * returning different pages to different backends in a round-robin fashion.
     * fp_next_slot points to the next slot to be returned (assuming there's
     * enough space on it for the request).  It's defined as an int because it's
     * updated without an exclusive lock; uint16 would be more appropriate, but
     * int is more likely to be atomically fetchable/storable.
     */
    pub fp_next_slot: c_int,

    /*
     * fp_nodes contains the binary tree, stored in array.  The first
     * NonLeafNodesPerPage elements are upper nodes, and the following
     * LeafNodesPerPage elements are leaf nodes.  Unused nodes are zero.
     * (FLEXIBLE_ARRAY_MEMBER -> zero-length trailing array.)
     */
    pub fp_nodes: [uint8; crate::c::FLEXIBLE_ARRAY_MEMBER],
}

pub type FSMPage = *mut FSMPageData;

/*
 * Number of non-leaf and leaf nodes, and nodes in total, on an FSM page.
 * These definitions are internal to fsmpage.c.
 *
 *   NodesPerPage = BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - offsetof(FSMPageData, fp_nodes)
 */
pub const NodesPerPage: usize =
    BLCKSZ - MAXALIGN(SizeOfPageHeaderData) - offset_of!(FSMPageData, fp_nodes);

pub const NonLeafNodesPerPage: usize = BLCKSZ / 2 - 1;
pub const LeafNodesPerPage: usize = NodesPerPage - NonLeafNodesPerPage;

/*
 * Number of FSM "slots" on a FSM page.  This is what should be used outside
 * fsmpage.c.
 */
pub const SlotsPerFSMPage: usize = LeafNodesPerPage;

// ----------------------------------------------------------------------------
//   freespace.c category mapping (reproduced so the page logic is self-contained)
// ----------------------------------------------------------------------------

pub const FSM_CATEGORIES: usize = 256;
pub const FSM_CAT_STEP: usize = BLCKSZ / FSM_CATEGORIES;
/* MaxFSMRequestSize is MaxHeapTupleSize; see freespace.c. */
pub const MaxFSMRequestSize: usize = MaxHeapTupleSize;

/*
 * Return category corresponding to `avail` bytes of free space.
 */
pub fn fsm_space_avail_to_cat(avail: Size) -> uint8 {
    Assert!(avail < BLCKSZ);

    if avail >= MaxFSMRequestSize {
        return 255;
    }

    let mut cat: c_int = (avail / FSM_CAT_STEP) as c_int;

    /*
     * The highest category, 255, is reserved for MaxFSMRequestSize bytes or
     * more.
     */
    if cat > 254 {
        cat = 254;
    }

    cat as uint8
}

/*
 * Return the lower bound of the range of free space represented by given
 * category.
 */
pub fn fsm_space_cat_to_avail(cat: uint8) -> Size {
    /* The highest category represents exactly MaxFSMRequestSize bytes. */
    if cat == 255 {
        MaxFSMRequestSize
    } else {
        (cat as usize) * FSM_CAT_STEP
    }
}

// ----------------------------------------------------------------------------
//   Tree navigation macros (Root has index zero.)
// ----------------------------------------------------------------------------

#[inline]
fn leftchild(x: c_int) -> c_int {
    2 * x + 1
}
#[inline]
fn rightchild(x: c_int) -> c_int {
    2 * x + 2
}
#[inline]
fn parentof(x: c_int) -> c_int {
    (x - 1) / 2
}

/*
 * Find right neighbor of x, wrapping around within the level.
 */
fn rightneighbor(mut x: c_int) -> c_int {
    /*
     * Move right.  This might wrap around, stepping to the leftmost node at the
     * next level.
     */
    x += 1;

    /*
     * Check if we stepped to the leftmost node at next level, and correct if
     * so.  The leftmost nodes at each level are numbered x = 2^level - 1, so
     * check if (x + 1) is a power of two, using a standard twos-complement
     * arithmetic trick.
     */
    if ((x + 1) & x) == 0 {
        x = parentof(x);
    }

    x
}

// ----------------------------------------------------------------------------
//   FSM page accessor (fsm_internals.h FSMPageGetContents lives only via casts;
//   the page contents start at PageGetContents(page))
// ----------------------------------------------------------------------------

/*
 * Reinterpret a page's contents area as an FSMPage.  (In C this is the bare
 * cast `(FSMPage) PageGetContents(page)`.)
 */
#[inline]
unsafe fn FSMPageGetContents(page: Page) -> FSMPage {
    PageGetContents(page) as FSMPage
}

/* Read/write helpers for the flexible fp_nodes[] array. */
#[inline]
unsafe fn node(fsmpage: FSMPage, idx: c_int) -> uint8 {
    *(*fsmpage).fp_nodes.as_ptr().add(idx as usize)
}
#[inline]
unsafe fn set_node(fsmpage: FSMPage, idx: c_int, value: uint8) {
    *(*fsmpage).fp_nodes.as_mut_ptr().add(idx as usize) = value;
}

// ----------------------------------------------------------------------------
//   Public API (fsmpage.c)
// ----------------------------------------------------------------------------

/*
 * Sets the value of a slot on page.  Returns true if the page was modified.
 *
 * The caller must hold an exclusive lock on the page.
 */
pub unsafe fn fsm_set_avail(page: Page, slot: c_int, value: uint8) -> bool {
    let mut nodeno: c_int = NonLeafNodesPerPage as c_int + slot;
    let fsmpage: FSMPage = FSMPageGetContents(page);

    Assert!(slot < LeafNodesPerPage as c_int);

    let mut oldvalue: uint8 = node(fsmpage, nodeno);

    /* If the value hasn't changed, we don't need to do anything */
    if oldvalue == value && value <= node(fsmpage, 0) {
        return false;
    }

    set_node(fsmpage, nodeno, value);

    /*
     * Propagate up, until we hit the root or a node that doesn't need to be
     * updated.
     */
    loop {
        nodeno = parentof(nodeno);
        let lchild = leftchild(nodeno);
        let rchild = lchild + 1;

        let mut newvalue: uint8 = node(fsmpage, lchild);
        if rchild < NodesPerPage as c_int {
            newvalue = Max(newvalue, node(fsmpage, rchild));
        }

        oldvalue = node(fsmpage, nodeno);
        if oldvalue == newvalue {
            break;
        }

        set_node(fsmpage, nodeno, newvalue);

        if !(nodeno > 0) {
            break;
        }
    }

    /*
     * sanity check: if the new value is (still) higher than the value at the
     * top, the tree is corrupt.  If so, rebuild.
     */
    if value > node(fsmpage, 0) {
        fsm_rebuild_page(page);
    }

    true
}

/*
 * Returns the value of given slot on page.
 *
 * Since this is just a read-only access of a single byte, the page doesn't need
 * to be locked.
 */
pub unsafe fn fsm_get_avail(page: Page, slot: c_int) -> uint8 {
    let fsmpage: FSMPage = FSMPageGetContents(page);

    Assert!(slot < LeafNodesPerPage as c_int);

    node(fsmpage, NonLeafNodesPerPage as c_int + slot)
}

/*
 * Returns the value at the root of a page.
 *
 * Since this is just a read-only access of a single byte, the page doesn't need
 * to be locked.
 */
pub unsafe fn fsm_get_max_avail(page: Page) -> uint8 {
    let fsmpage: FSMPage = FSMPageGetContents(page);
    node(fsmpage, 0)
}

/*
 * Searches for a slot with category at least minvalue.
 * Returns slot number, or -1 if none found.
 *
 * The caller must hold at least a shared lock on the page, and this function
 * can unlock and lock the page again in exclusive mode if it needs to be
 * updated.  exclusive_lock_held should be set to true if the caller is already
 * holding an exclusive lock, to avoid extra work.
 *
 * If advancenext is false, fp_next_slot is set to point to the returned slot,
 * and if it's true, to the slot after the returned slot.
 */
pub unsafe fn fsm_search_avail(
    buf: Buffer,
    minvalue: uint8,
    advancenext: bool,
    mut exclusive_lock_held: bool,
) -> c_int {
    let page: Page = BufferGetPage(buf);
    let fsmpage: FSMPage = FSMPageGetContents(page);

    // 'restart:' goto target -> outer loop.
    loop {
        /*
         * Check the root first, and exit quickly if there's no leaf with enough
         * free space
         */
        if node(fsmpage, 0) < minvalue {
            return -1;
        }

        /*
         * Start search using fp_next_slot.  It's just a hint, so check that
         * it's sane.  (This also handles wrapping around when the prior call
         * returned the last slot on the page.)
         */
        let mut target: c_int = (*fsmpage).fp_next_slot;
        if target < 0 || target >= LeafNodesPerPage as c_int {
            target = 0;
        }
        target += NonLeafNodesPerPage as c_int;

        /*----------
         * Start the search from the target slot.  At every step, move one node
         * to the right, then climb up to the parent.  Stop when we reach a node
         * with enough free space (as we must, since the root has enough space).
         *
         * The idea is to gradually expand our "search triangle", that is, all
         * nodes covered by the current node, and to be sure we search to the
         * right from the start point.  See fsmpage.c for the full worked
         * example.  Each step doubles the size of the search triangle, so the
         * whole search is O(log N).  The "move right" wraps around at the right
         * edge of the tree, and the move-and-climb behavior ensures we never
         * land on one of the missing nodes at the right of the leaf level.
         *----------
         */
        let mut nodeno: c_int = target;
        while nodeno > 0 {
            if node(fsmpage, nodeno) >= minvalue {
                break;
            }

            /*
             * Move to the right, wrapping around on same level if necessary,
             * then climb up.
             */
            nodeno = parentof(rightneighbor(nodeno));
        }

        /*
         * We're now at a node with enough free space, somewhere in the middle
         * of the tree.  Descend to the bottom, following a path with enough
         * free space, preferring to move left if there's a choice.
         */
        let mut torn_page = false;
        while nodeno < NonLeafNodesPerPage as c_int {
            let mut childnodeno: c_int = leftchild(nodeno);

            if childnodeno < NodesPerPage as c_int && node(fsmpage, childnodeno) >= minvalue {
                nodeno = childnodeno;
                continue;
            }
            childnodeno += 1; /* point to right child */
            if childnodeno < NodesPerPage as c_int && node(fsmpage, childnodeno) >= minvalue {
                nodeno = childnodeno;
            } else {
                /*
                 * Oops.  The parent node promised that either left or right
                 * child has enough space, but neither actually did.  This can
                 * happen in case of a "torn page", IOW if we crashed earlier
                 * while writing the page to disk, and only part of the page
                 * made it to disk.
                 *
                 * Fix the corruption and restart.
                 */
                let mut rlocator: crate::common::blkreftable::RelFileLocator =
                    core::mem::zeroed();
                let mut forknum: crate::common::relpath::ForkNumber = core::mem::zeroed();
                let mut blknum: BlockNumber = 0;

                BufferGetTag(buf, &mut rlocator, &mut forknum, &mut blknum);
                elog!(
                    DEBUG1,
                    "fixing corrupt FSM block {}, relation {}/{}/{}",
                    blknum,
                    rlocator.spcOid,
                    rlocator.dbOid,
                    rlocator.relNumber
                );

                /* make sure we hold an exclusive lock */
                if !exclusive_lock_held {
                    LockBuffer(buf, BUFFER_LOCK_UNLOCK);
                    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
                    exclusive_lock_held = true;
                }
                fsm_rebuild_page(page);
                MarkBufferDirtyHint(buf, false);
                torn_page = true;
                break;
            }
        }
        if torn_page {
            continue; // goto restart;
        }

        /* We're now at the bottom level, at a node with enough space. */
        let slot: uint16 = (nodeno - NonLeafNodesPerPage as c_int) as uint16;

        /*
         * Update the next-target pointer.  Note that we do this even if we're
         * only holding a shared lock, on the grounds that it's better to use a
         * shared lock and get a garbled next pointer every now and then, than
         * take the concurrency hit of an exclusive lock.
         *
         * Wrap-around is handled at the beginning of this function.
         */
        (*fsmpage).fp_next_slot = slot as c_int + if advancenext { 1 } else { 0 };

        return slot as c_int;
    }
}

/*
 * Sets the available space to zero for all slots numbered >= nslots.
 * Returns true if the page was modified.
 */
pub unsafe fn fsm_truncate_avail(page: Page, nslots: c_int) -> bool {
    let fsmpage: FSMPage = FSMPageGetContents(page);
    let mut changed = false;

    Assert!(nslots >= 0 && nslots < LeafNodesPerPage as c_int);

    /* Clear all truncated leaf nodes */
    let base = (*fsmpage).fp_nodes.as_mut_ptr();
    let mut ptr = base.add(NonLeafNodesPerPage + nslots as usize);
    let end = base.add(NodesPerPage);
    while ptr < end {
        if *ptr != 0 {
            changed = true;
        }
        *ptr = 0;
        ptr = ptr.add(1);
    }

    /* Fix upper nodes. */
    if changed {
        fsm_rebuild_page(page);
    }

    changed
}

/*
 * Reconstructs the upper levels of a page.  Returns true if the page was
 * modified.
 */
pub unsafe fn fsm_rebuild_page(page: Page) -> bool {
    let fsmpage: FSMPage = FSMPageGetContents(page);
    let mut changed = false;

    /*
     * Start from the lowest non-leaf level, at last node, working our way
     * backwards, through all non-leaf nodes at all levels, up to the root.
     */
    let mut nodeno: c_int = NonLeafNodesPerPage as c_int - 1;
    while nodeno >= 0 {
        let lchild = leftchild(nodeno);
        let rchild = lchild + 1;
        let mut newvalue: uint8 = 0;

        /* The first few nodes we examine might have zero or one child. */
        if lchild < NodesPerPage as c_int {
            newvalue = node(fsmpage, lchild);
        }

        if rchild < NodesPerPage as c_int {
            newvalue = Max(newvalue, node(fsmpage, rchild));
        }

        if node(fsmpage, nodeno) != newvalue {
            set_node(fsmpage, nodeno, newvalue);
            changed = true;
        }

        nodeno -= 1;
    }

    changed
}

// ----------------------------------------------------------------------------
//   Tests
// ----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /*
     * Allocate a zeroed BLCKSZ page buffer and set pd_lower so PageGetContents
     * lands at MAXALIGN(SizeOfPageHeaderData).  Since PageGetContents only uses
     * the header size (not pd_lower), a plain zeroed buffer is sufficient.
     */
    fn make_page() -> Vec<u8> {
        vec![0u8; BLCKSZ]
    }

    #[test]
    fn perpage_invariants() {
        /* The leaf level must hold at least as many slots as half the tree. */
        assert!(LeafNodesPerPage > 0);
        assert_eq!(NonLeafNodesPerPage, BLCKSZ / 2 - 1);
        assert_eq!(NodesPerPage, NonLeafNodesPerPage + LeafNodesPerPage);
    }

    #[test]
    fn set_get_roundtrip() {
        let mut buf = make_page();
        let page = buf.as_mut_ptr() as Page;
        unsafe {
            assert_eq!(fsm_get_avail(page, 0), 0);
            assert!(fsm_set_avail(page, 0, 42));
            assert_eq!(fsm_get_avail(page, 0), 42);

            /* A second slot, independent of the first. */
            assert!(fsm_set_avail(page, 5, 100));
            assert_eq!(fsm_get_avail(page, 5), 100);
            assert_eq!(fsm_get_avail(page, 0), 42);

            /* Setting the same value again is a no-op (returns false). */
            assert!(!fsm_set_avail(page, 5, 100));
        }
    }

    #[test]
    fn max_avail_reflects_max() {
        let mut buf = make_page();
        let page = buf.as_mut_ptr() as Page;
        unsafe {
            assert_eq!(fsm_get_max_avail(page), 0);
            fsm_set_avail(page, 0, 10);
            assert_eq!(fsm_get_max_avail(page), 10);
            fsm_set_avail(page, 1, 30);
            assert_eq!(fsm_get_max_avail(page), 30);
            fsm_set_avail(page, 2, 20);
            assert_eq!(fsm_get_max_avail(page), 30);
            /* Lowering the current max leaf brings the root down. */
            fsm_set_avail(page, 1, 5);
            assert_eq!(fsm_get_max_avail(page), 20);
        }
    }

    /*
     * Direct descent test of fsm_search_avail's inner logic without a Buffer.
     * We replicate the tree-descent the same way fsm_search_avail does, but on
     * a raw page, by calling fsm_set_avail and then walking the public root /
     * leaf accessors.  To exercise the real search, we test via a tiny shim
     * that mirrors the body but takes a Page (BufferGetPage is unported).
     */
    unsafe fn search_on_page(page: Page, minvalue: uint8, advancenext: bool) -> c_int {
        let fsmpage: FSMPage = FSMPageGetContents(page);
        if node(fsmpage, 0) < minvalue {
            return -1;
        }
        let mut target: c_int = (*fsmpage).fp_next_slot;
        if target < 0 || target >= LeafNodesPerPage as c_int {
            target = 0;
        }
        target += NonLeafNodesPerPage as c_int;

        let mut nodeno: c_int = target;
        while nodeno > 0 {
            if node(fsmpage, nodeno) >= minvalue {
                break;
            }
            nodeno = parentof(rightneighbor(nodeno));
        }
        while nodeno < NonLeafNodesPerPage as c_int {
            let mut childnodeno = leftchild(nodeno);
            if childnodeno < NodesPerPage as c_int && node(fsmpage, childnodeno) >= minvalue {
                nodeno = childnodeno;
                continue;
            }
            childnodeno += 1;
            if childnodeno < NodesPerPage as c_int && node(fsmpage, childnodeno) >= minvalue {
                nodeno = childnodeno;
            } else {
                return -2; /* torn page; not expected in tests */
            }
        }
        let slot = (nodeno - NonLeafNodesPerPage as c_int) as uint16;
        (*fsmpage).fp_next_slot = slot as c_int + if advancenext { 1 } else { 0 };
        slot as c_int
    }

    #[test]
    fn search_finds_slot_with_enough_space() {
        let mut buf = make_page();
        let page = buf.as_mut_ptr() as Page;
        unsafe {
            /* Empty page: no slot satisfies a positive request. */
            assert_eq!(search_on_page(page, 1, false), -1);

            /* Put space on a handful of slots. */
            fsm_set_avail(page, 3, 10);
            fsm_set_avail(page, 7, 50);
            fsm_set_avail(page, 9, 30);

            /* A request that only slot 7 can satisfy must return slot 7. */
            assert_eq!(search_on_page(page, 50, false), 7);

            /* A request of 30: slots 7 and 9 qualify; either is acceptable. */
            let s = search_on_page(page, 30, false);
            assert!(s == 7 || s == 9, "got slot {}", s);

            /* A request larger than any leaf returns -1. */
            assert_eq!(search_on_page(page, 200, false), -1);
        }
    }

    #[test]
    fn truncate_clears_high_slots() {
        let mut buf = make_page();
        let page = buf.as_mut_ptr() as Page;
        unsafe {
            fsm_set_avail(page, 0, 10);
            fsm_set_avail(page, 4, 90);
            fsm_set_avail(page, 8, 40);
            assert_eq!(fsm_get_max_avail(page), 90);

            /* Truncate everything from slot 4 up: slot 0 survives, 4 and 8 die. */
            assert!(fsm_truncate_avail(page, 4));
            assert_eq!(fsm_get_avail(page, 0), 10);
            assert_eq!(fsm_get_avail(page, 4), 0);
            assert_eq!(fsm_get_avail(page, 8), 0);
            assert_eq!(fsm_get_max_avail(page), 10);

            /* Truncating again when nothing changes returns false. */
            assert!(!fsm_truncate_avail(page, 4));
        }
    }

    #[test]
    fn rebuild_recomputes_upper_nodes() {
        let mut buf = make_page();
        let page = buf.as_mut_ptr() as Page;
        unsafe {
            let fsmpage = FSMPageGetContents(page);
            /* Poke leaf nodes directly, leaving upper nodes stale (zero). */
            set_node(fsmpage, NonLeafNodesPerPage as c_int + 0, 11);
            set_node(fsmpage, NonLeafNodesPerPage as c_int + 1, 77);
            assert_eq!(node(fsmpage, 0), 0); /* root still stale */

            assert!(fsm_rebuild_page(page));
            assert_eq!(fsm_get_max_avail(page), 77);

            /* A second rebuild with no change returns false. */
            assert!(!fsm_rebuild_page(page));
        }
    }

    #[test]
    fn category_mapping() {
        /* cat 0 maps to 0 bytes; round-trip lower bounds are monotone. */
        assert_eq!(fsm_space_cat_to_avail(0), 0);
        assert_eq!(fsm_space_avail_to_cat(0), 0);
        /* The top category is reserved for >= MaxFSMRequestSize. */
        assert_eq!(fsm_space_avail_to_cat(MaxFSMRequestSize), 255);
        assert_eq!(fsm_space_cat_to_avail(255), MaxFSMRequestSize);
        /* One FSM_CAT_STEP of free space is category 1. */
        assert_eq!(fsm_space_avail_to_cat(FSM_CAT_STEP), 1);
        /* avail_to_cat rounds down within a step. */
        assert_eq!(fsm_space_avail_to_cat(FSM_CAT_STEP + 1), 1);
        /* Categories never exceed 254 below the reserved top. */
        assert!(fsm_space_avail_to_cat(MaxFSMRequestSize - 1) <= 254);
    }
}
