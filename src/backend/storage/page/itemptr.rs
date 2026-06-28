//! Disk item pointer (TID) operations. Translated from backend/storage/page/itemptr.c.
//!
//! An item pointer, or TID, is the six-byte on-disk address of a heap tuple: a
//! block number plus an offset within that block. PostgreSQL keeps the simple
//! accessors and the set/get helpers inline in the header and places only the
//! non-trivial routines in this file: the equality test, the generic btree-style
//! three-way comparison, and the increment/decrement helpers that step a pointer
//! by one within the raw range of its component types (ignoring the usual
//! FirstOffsetNumber/MaxOffsetNumber bounds, so the result may be an invalid
//! offset). Comparison and equality use the unchecked block/offset accessors so
//! that a user-supplied TID with a zero offset does not trip a validity assertion.
//!
//! PepperDB is type-centric: these routines are methods on `ItemPointerData`
//! (`equals`, `compare`, `inc`, `dec`) rather than free functions, while the
//! header retains the inline accessors and C-named shims. All operations are
//! pure synchronous value computations with no I/O or shared state.

use crate::c::PG_UINT16_MAX;
use crate::storage::block::INVALID_BLOCK_NUMBER;
use crate::storage::itemptr::ItemPointerData;

impl ItemPointerData {
    /// True iff both item pointers point to the same item (same block and offset).
    /// (C ItemPointerEquals; asserts both are valid, hence the *_no_check reads.)
    pub fn equals(&self, other: &Self) -> bool {
        self.block_number_no_check() == other.block_number_no_check()
            && self.offset_number_no_check() == other.offset_number_no_check()
    }

    /// Generic btree-style comparison: block number first, then offset number.
    /// Returns <0, 0, or >0. Uses the *_no_check accessors so a user-supplied TID
    /// with ip_posid == 0 does not trip the validity assert.
    pub fn compare(&self, other: &Self) -> i32 {
        use core::cmp::Ordering;
        let b1 = self.block_number_no_check();
        let b2 = other.block_number_no_check();
        match b1.cmp(&b2) {
            Ordering::Less => -1,
            Ordering::Greater => 1,
            Ordering::Equal => {
                let o1 = self.offset_number_no_check();
                let o2 = other.offset_number_no_check();
                match o1.cmp(&o2) {
                    Ordering::Less => -1,
                    Ordering::Equal => 0,
                    Ordering::Greater => 1,
                }
            }
        }
    }

    /// Increment by 1, respecting only the types' range limits (not
    /// MaxOffsetNumber). May make the pointer invalid; no-op at the maximum.
    pub fn inc(&mut self) {
        let mut blk = self.block_number_no_check();
        let mut off = self.offset_number_no_check();
        if off == PG_UINT16_MAX {
            if blk != INVALID_BLOCK_NUMBER {
                off = 0;
                blk += 1;
            }
        } else {
            off += 1;
        }
        self.set(blk, off);
    }

    /// Decrement by 1, respecting only the types' range limits. May make the
    /// pointer invalid; no-op at the minimum. Relies on FirstOffsetNumber == 1.
    pub fn dec(&mut self) {
        let mut blk = self.block_number_no_check();
        let mut off = self.offset_number_no_check();
        if off == 0 {
            if blk != 0 {
                off = PG_UINT16_MAX;
                blk -= 1;
            }
        } else {
            off -= 1;
        }
        self.set(blk, off);
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::block::BlockIdData;
    use crate::storage::itemptr::ItemPointerData;

    fn tid(blk: u32, off: u16) -> ItemPointerData {
        let mut b = BlockIdData { hi: 0, lo: 0 };
        b.set(blk);
        ItemPointerData { blkid: b, posid: off }
    }

    #[test]
    fn compare_block_then_offset() {
        // Lower block sorts first regardless of offset.
        assert!(tid(1, 99).compare(&tid(2, 1)) < 0);
        assert!(tid(2, 1).compare(&tid(1, 99)) > 0);
        // Same block: offset breaks the tie.
        assert!(tid(5, 3).compare(&tid(5, 4)) < 0);
        assert!(tid(5, 4).compare(&tid(5, 3)) > 0);
        // Fully equal.
        assert_eq!(tid(5, 3).compare(&tid(5, 3)), 0);
    }

    #[test]
    fn equals_matches() {
        assert!(tid(7, 2).equals(&tid(7, 2)));
        assert!(!tid(7, 2).equals(&tid(7, 3)));
        assert!(!tid(7, 2).equals(&tid(8, 2)));
        // Offset 0 (would be invalid) still compares/equals via *_no_check.
        assert!(tid(0, 0).equals(&tid(0, 0)));
        assert_eq!(tid(0, 0).compare(&tid(0, 0)), 0);
    }
}
