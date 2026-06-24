//! Translated from PostgreSQL src/include/storage/checksum.h

use crate::storage::block::BlockNumber;

/// Compute the checksum for a Postgres page. The page must be 4-byte aligned.
pub fn pg_checksum_page(_page: &[u8], _blkno: BlockNumber) -> u16 {
    unimplemented!()
}
