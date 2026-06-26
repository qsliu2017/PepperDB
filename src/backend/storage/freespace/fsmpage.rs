//! Translated from PostgreSQL src/backend/storage/freespace/fsmpage.c -- search
//! and manipulation of a single FSM page.
//!
//! An FSM page stores a binary max-tree of 1-byte free-space categories in
//! `FSMPageData.fp_nodes`: the first `NON_LEAF_NODES_PER_PAGE` entries are upper
//! nodes, the next `LEAF_NODES_PER_PAGE` are the leaves (one per heap block this
//! page covers). Each upper node holds the max of its two children, so the root
//! (node 0) holds the max category on the page. `fsm_search_avail` descends the
//! tree to find a leaf with at least the requested category in O(log n).
//!
//! These are pure page-content operations (rules.md: type-centric -> methods on
//! a thin [`FsmPage`] view over the page bytes). The caller holds the buffer
//! content lock for the duration; nothing here `.await`s. The header module
//! `src/storage/fsm_internals.rs` keeps the C-named free functions as
//! `#[deprecated]` shims over these methods.

use crate::c::MAXALIGN;
use crate::storage::bufpage::{Page, SizeOfPageHeaderData};
use crate::storage::fsm_internals::{
    LEAF_NODES_PER_PAGE, NODES_PER_PAGE, NON_LEAF_NODES_PER_PAGE, SIZE_OF_FSM_PAGE_DATA,
};

/// Byte offset of `FSMPageData` within a page (C: `PageGetContents(page)`).
const FSM_CONTENTS_OFFSET: usize = MAXALIGN(SizeOfPageHeaderData);
/// Byte offset of `fp_nodes[0]` within a page.
const FSM_NODES_OFFSET: usize = FSM_CONTENTS_OFFSET + SIZE_OF_FSM_PAGE_DATA;

/* Tree navigation. Root has index zero. */
#[inline]
const fn leftchild(x: usize) -> usize {
    2 * x + 1
}
#[inline]
const fn parentof(x: usize) -> usize {
    (x - 1) / 2
}

/// Right neighbor of `x`, wrapping around within the level. C: `rightneighbor`.
#[inline]
fn rightneighbor(mut x: usize) -> usize {
    x += 1;
    // Stepped to the leftmost node of the next level? Leftmost nodes are
    // numbered 2^level - 1, so (x + 1) & x == 0 detects a power of two.
    if ((x + 1) & x) == 0 {
        x = parentof(x);
    }
    x
}

/// A thin view over a page's `FSMPageData` region. Pure byte logic; the caller
/// holds the buffer content lock.
pub struct FsmPage<'a>(&'a Page);

/// Mutable variant of [`FsmPage`].
pub struct FsmPageMut<'a>(&'a mut Page);

impl<'a> FsmPage<'a> {
    #[inline]
    pub fn new(page: &'a Page) -> Self {
        FsmPage(page)
    }

    #[inline]
    fn node(&self, nodeno: usize) -> u8 {
        self.0.as_bytes()[FSM_NODES_OFFSET + nodeno]
    }

    #[inline]
    fn next_slot(&self) -> i32 {
        let b = &self.0.as_bytes()[FSM_CONTENTS_OFFSET..FSM_CONTENTS_OFFSET + 4];
        i32::from_ne_bytes([b[0], b[1], b[2], b[3]])
    }

    /// C: `fsm_get_avail`. The category of a leaf slot.
    #[inline]
    pub fn get_avail(&self, slot: usize) -> u8 {
        debug_assert!(slot < LEAF_NODES_PER_PAGE);
        self.node(NON_LEAF_NODES_PER_PAGE + slot)
    }

    /// C: `fsm_get_max_avail`. The category at the root (max on the page).
    #[inline]
    pub fn get_max_avail(&self) -> u8 {
        self.node(0)
    }

    /// Read-only variant of [`FsmPageMut::search_avail`]: descend the max-tree
    /// for a slot with category >= `minvalue` using ONLY `&Page`, never writing
    /// the `fp_next_slot` hint. Sound under the SHARED content lock -- the tree
    /// nodes (`fp_nodes`) are written only under the exclusive lock, so reading
    /// them here is stable. Returns the slot, or `None`.
    ///
    /// Unlike the mutating version this cannot rebuild a torn page (that needs
    /// the exclusive lock); a torn page is treated as "no slot found" (`None`),
    /// which the caller's tree descent self-heals on the exclusive-lock path.
    pub fn search_avail(&self, minvalue: u8) -> Option<usize> {
        // Quick exit if the root hasn't got enough free space anywhere.
        if self.node(0) < minvalue {
            return None;
        }

        // Start from fp_next_slot (a hint; sanitize it). Read-only: we never
        // write it back.
        let mut target = self.next_slot();
        if target < 0 || target as usize >= LEAF_NODES_PER_PAGE {
            target = 0;
        }
        let mut nodeno = target as usize + NON_LEAF_NODES_PER_PAGE;

        // Climb: move right (wrapping), then up, until a node has enough.
        while nodeno > 0 {
            if self.node(nodeno) >= minvalue {
                break;
            }
            nodeno = parentof(rightneighbor(nodeno));
        }

        // Descend to a leaf, preferring left.
        while nodeno < NON_LEAF_NODES_PER_PAGE {
            let mut childnodeno = leftchild(nodeno);
            if childnodeno < NODES_PER_PAGE && self.node(childnodeno) >= minvalue {
                nodeno = childnodeno;
                continue;
            }
            childnodeno += 1; // right child
            if childnodeno < NODES_PER_PAGE && self.node(childnodeno) >= minvalue {
                nodeno = childnodeno;
            } else {
                // Torn page: parent promised space neither child has. We cannot
                // rebuild under the shared lock; report no slot found.
                return None;
            }
        }

        Some(nodeno - NON_LEAF_NODES_PER_PAGE)
    }
}

impl<'a> FsmPageMut<'a> {
    #[inline]
    pub fn new(page: &'a mut Page) -> Self {
        FsmPageMut(page)
    }

    #[inline]
    fn node(&self, nodeno: usize) -> u8 {
        self.0.as_bytes()[FSM_NODES_OFFSET + nodeno]
    }

    #[inline]
    fn set_node(&mut self, nodeno: usize, value: u8) {
        self.0.as_mut_bytes()[FSM_NODES_OFFSET + nodeno] = value;
    }

    #[inline]
    fn next_slot(&self) -> i32 {
        let b = &self.0.as_bytes()[FSM_CONTENTS_OFFSET..FSM_CONTENTS_OFFSET + 4];
        i32::from_ne_bytes([b[0], b[1], b[2], b[3]])
    }

    #[inline]
    fn set_next_slot(&mut self, v: i32) {
        let bytes = v.to_ne_bytes();
        self.0.as_mut_bytes()[FSM_CONTENTS_OFFSET..FSM_CONTENTS_OFFSET + 4].copy_from_slice(&bytes);
    }

    #[inline]
    pub fn get_avail(&self, slot: usize) -> u8 {
        debug_assert!(slot < LEAF_NODES_PER_PAGE);
        self.node(NON_LEAF_NODES_PER_PAGE + slot)
    }

    #[inline]
    pub fn get_max_avail(&self) -> u8 {
        self.node(0)
    }

    /// C: `fsm_set_avail`. Set the category of a leaf slot and bubble the max up
    /// the tree. Returns true if the page was modified. The caller must hold an
    /// exclusive lock on the page.
    pub fn set_avail(&mut self, slot: usize, value: u8) -> bool {
        debug_assert!(slot < LEAF_NODES_PER_PAGE);
        let mut nodeno = NON_LEAF_NODES_PER_PAGE + slot;

        let oldvalue = self.node(nodeno);
        // Unchanged and not above the root -> nothing to do.
        if oldvalue == value && value <= self.node(0) {
            return false;
        }

        self.set_node(nodeno, value);

        // Propagate up until the root or a node that needs no update.
        loop {
            nodeno = parentof(nodeno);
            let lchild = leftchild(nodeno);
            let rchild = lchild + 1;

            let mut newvalue = self.node(lchild);
            if rchild < NODES_PER_PAGE {
                newvalue = newvalue.max(self.node(rchild));
            }

            if self.node(nodeno) == newvalue {
                break;
            }
            self.set_node(nodeno, newvalue);
            if nodeno == 0 {
                break;
            }
        }

        // If the new value still exceeds the root, the tree is corrupt: rebuild.
        if value > self.node(0) {
            self.rebuild_page();
        }

        true
    }

    /// C: `fsm_search_avail`. Search for a slot with category >= `minvalue`,
    /// returning its slot number, or `None` if none. Updates `fp_next_slot`.
    ///
    /// In C this can detect a torn page (a parent promising space neither child
    /// has) and rebuild + restart; that needs the buffer for `BufferGetTag` and a
    /// possible relock. Here the page bytes are self-contained: on the rare
    /// torn-page case we rebuild the upper levels in place and restart (the
    /// caller already holds the exclusive lock in that path; `exclusive_lock_held`
    /// is accepted for parity but the rebuild is always safe here).
    pub fn search_avail(&mut self, minvalue: u8, advancenext: bool) -> Option<usize> {
        loop {
            // Quick exit if the root hasn't got enough free space anywhere.
            if self.node(0) < minvalue {
                return None;
            }

            // Start from fp_next_slot (a hint; sanitize it).
            let mut target = self.next_slot();
            if target < 0 || target as usize >= LEAF_NODES_PER_PAGE {
                target = 0;
            }
            let mut nodeno = target as usize + NON_LEAF_NODES_PER_PAGE;

            // Climb: move right (wrapping), then up, until a node has enough.
            while nodeno > 0 {
                if self.node(nodeno) >= minvalue {
                    break;
                }
                nodeno = parentof(rightneighbor(nodeno));
            }

            // Descend to a leaf, preferring left.
            let mut torn = false;
            while nodeno < NON_LEAF_NODES_PER_PAGE {
                let mut childnodeno = leftchild(nodeno);
                if childnodeno < NODES_PER_PAGE && self.node(childnodeno) >= minvalue {
                    nodeno = childnodeno;
                    continue;
                }
                childnodeno += 1; // right child
                if childnodeno < NODES_PER_PAGE && self.node(childnodeno) >= minvalue {
                    nodeno = childnodeno;
                } else {
                    // Torn page: parent promised space neither child has. Fix and
                    // restart.
                    torn = true;
                    break;
                }
            }

            if torn {
                self.rebuild_page();
                continue;
            }

            // At a leaf with enough space.
            let slot = nodeno - NON_LEAF_NODES_PER_PAGE;
            self.set_next_slot(slot as i32 + i32::from(advancenext));
            return Some(slot);
        }
    }

    /// C: `fsm_truncate_avail`. Zero the available space for all slots >=
    /// `nslots`, then rebuild upper nodes. Returns true if the page was modified.
    pub fn truncate_avail(&mut self, nslots: usize) -> bool {
        debug_assert!(nslots < LEAF_NODES_PER_PAGE);
        let mut changed = false;
        for nodeno in (NON_LEAF_NODES_PER_PAGE + nslots)..NODES_PER_PAGE {
            if self.node(nodeno) != 0 {
                changed = true;
                self.set_node(nodeno, 0);
            }
        }
        if changed {
            self.rebuild_page();
        }
        changed
    }

    /// C: `fsm_rebuild_page`. Reconstruct the upper levels from the leaves.
    /// Returns true if the page was modified.
    pub fn rebuild_page(&mut self) -> bool {
        let mut changed = false;
        // From the lowest non-leaf level back to the root.
        for nodeno in (0..NON_LEAF_NODES_PER_PAGE).rev() {
            let lchild = leftchild(nodeno);
            let rchild = lchild + 1;
            let mut newvalue = if lchild < NODES_PER_PAGE {
                self.node(lchild)
            } else {
                0u8
            };
            if rchild < NODES_PER_PAGE {
                newvalue = newvalue.max(self.node(rchild));
            }
            if self.node(nodeno) != newvalue {
                self.set_node(nodeno, newvalue);
                changed = true;
            }
        }
        changed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pg_config::BLCKSZ;
    use crate::storage::fsm_internals::SLOTS_PER_FSM_PAGE;

    fn fresh_page() -> Box<Page> {
        let mut p = Page::boxed_zeroed();
        p.init(BLCKSZ as usize, 0);
        p
    }

    #[test]
    fn set_then_search_finds_slot() {
        let mut page = fresh_page();
        let mut fp = FsmPageMut::new(&mut page);
        assert!(fp.set_avail(100, 50));
        assert_eq!(fp.get_avail(100), 50);
        // Search for category 40 -> finds slot 100 (the only non-zero).
        assert_eq!(fp.search_avail(40, false), Some(100));
        // Search for more than available -> nothing.
        assert_eq!(fp.search_avail(60, false), None);
    }

    #[test]
    fn max_tree_bubbles_up() {
        let mut page = fresh_page();
        let mut fp = FsmPageMut::new(&mut page);
        fp.set_avail(0, 10);
        fp.set_avail(5, 99);
        fp.set_avail(SLOTS_PER_FSM_PAGE - 1, 30);
        // Root holds the max category on the page.
        assert_eq!(fp.get_max_avail(), 99);
        // Lower a leaf below the others: root drops to the next max.
        fp.set_avail(5, 0);
        assert_eq!(fp.get_max_avail(), 30);
    }

    #[test]
    fn search_respects_minvalue_and_advancenext() {
        let mut page = fresh_page();
        let mut fp = FsmPageMut::new(&mut page);
        fp.set_avail(3, 200);
        fp.set_avail(7, 200);
        // advancenext moves fp_next_slot past the returned slot.
        let s1 = fp.search_avail(100, true).unwrap();
        let s2 = fp.search_avail(100, true).unwrap();
        assert!(s1 == 3 || s1 == 7);
        assert!(s2 == 3 || s2 == 7);
        // Two distinct high slots -> the round-robin returns both.
        assert_ne!(s1, s2);
    }

    #[test]
    fn truncate_clears_tail_and_fixes_tree() {
        let mut page = fresh_page();
        let mut fp = FsmPageMut::new(&mut page);
        fp.set_avail(10, 80);
        fp.set_avail(200, 90);
        assert_eq!(fp.get_max_avail(), 90);
        // Truncate at slot 50: slot 200 is cleared, slot 10 survives.
        assert!(fp.truncate_avail(50));
        assert_eq!(fp.get_avail(10), 80);
        assert_eq!(fp.get_avail(200), 0);
        assert_eq!(fp.get_max_avail(), 80);
    }

    #[test]
    fn read_only_search_matches_mutating_search() {
        // FsmPage::search_avail (the &Page variant used under the shared lock)
        // finds the same slot the mutating FsmPageMut::search_avail does, without
        // touching fp_next_slot.
        let mut page = fresh_page();
        {
            let mut fp = FsmPageMut::new(&mut page);
            fp.set_avail(42, 70);
            fp.set_avail(900, 90);
        }
        // Snapshot the whole page; the read-only search must not mutate it.
        let before = page.as_bytes().to_vec();
        let view = FsmPage::new(&page);
        // Finds a slot with enough space.
        let s = view.search_avail(60).expect("a slot with cat >= 60 exists");
        assert!(s == 42 || s == 900);
        // Asking for more than the max -> None.
        assert_eq!(view.search_avail(100), None);
        // The page bytes are unchanged (no fp_next_slot write).
        assert_eq!(page.as_bytes(), before.as_slice());
    }

    #[test]
    fn rebuild_recomputes_from_leaves() {
        let mut page = fresh_page();
        let mut fp = FsmPageMut::new(&mut page);
        fp.set_avail(1, 42);
        // Corrupt the root by hand, then rebuild.
        fp.set_node(0, 0);
        assert!(fp.rebuild_page());
        assert_eq!(fp.get_max_avail(), 42);
    }
}
