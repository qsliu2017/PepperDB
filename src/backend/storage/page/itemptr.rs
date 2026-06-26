//! Translated from PostgreSQL src/backend/storage/page/itemptr.c
//! POSTGRES disk item pointer code.
//!
//! Only the two non-inline functions from itemptr.c live here: the equality test
//! and the btree-style 3-way compare. The increment/decrement/get/set operations
//! are inline in the header (src/storage/itemptr.rs). Type-centric, so these are
//! idiomatic methods on ItemPointerData; the header keeps deprecated C-named shims.
//!
//! Pure synchronous value operations.

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
