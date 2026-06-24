//! Translated from PostgreSQL src/include/access/tidstore.h
//! TidStore interface. In-memory; shared-memory (DSA) variants collapse under
//! the single-process model and are omitted (utils/dsa.h is a tombstone).

use crate::storage::block::BlockNumber;
use crate::storage::itemptr::ItemPointerData;
use crate::storage::off::OffsetNumber;

/// Opaque store of TIDs (a radix tree keyed by block + offset bitmap).
pub struct TidStore {
    _private: (),
}

/// Opaque iterator over a TidStore.
pub struct TidStoreIter {
    _private: (),
}

/// Result of TidStoreIterateNext. Copyable but treated as opaque; call
/// `get_block_offsets` to obtain the offsets.
#[derive(Clone, Copy)]
pub struct TidStoreIterResult {
    pub blkno: BlockNumber,
    pub internal_page: *const (), // TODO(ptr): opaque internal radix-tree page
}

impl TidStore {
    /// Local (process-private) store with a memory budget.
    pub fn create_local(_max_bytes: usize, _insert_only: bool) -> Box<TidStore> {
        unimplemented!()
    }

    // C exposes Lock{Exclusive,Share}/Unlock for the shared variant; under the
    // single-process model the store is owned, so these become no-ops / are
    // expressed through &/&mut borrows. Kept as stubs for API parity.
    pub fn lock_exclusive(&mut self) {
        unimplemented!()
    }
    pub fn lock_share(&self) {
        unimplemented!()
    }
    pub fn unlock(&self) {
        unimplemented!()
    }

    pub fn set_block_offsets(
        &mut self,
        _blkno: BlockNumber,
        _offsets: &[OffsetNumber],
    ) {
        unimplemented!()
    }

    pub fn is_member(&self, _tid: &ItemPointerData) -> bool {
        unimplemented!()
    }

    pub fn begin_iterate(&self) -> Box<TidStoreIter> {
        unimplemented!()
    }

    pub fn memory_usage(&self) -> usize {
        unimplemented!()
    }
}

impl TidStoreIter {
    /// Returns the next result, or None when iteration is exhausted.
    pub fn next(&mut self) -> Option<TidStoreIterResult> {
        unimplemented!()
    }
}

impl TidStoreIterResult {
    /// Copies the offsets for this block into `offsets`, returning the count.
    pub fn get_block_offsets(&self, _offsets: &mut [OffsetNumber]) -> i32 {
        unimplemented!()
    }
}
