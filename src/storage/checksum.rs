//! Translated from PostgreSQL src/include/storage/checksum.h

use crate::storage::block::BlockNumber;
use crate::storage::bufpage::Page;

// The algorithm (checksum_impl.h) is the `Page::checksum` method in the backend
// module; keep the C-named free function as a deprecated delegating shim.

#[deprecated(note = "use `page.checksum(blkno)`")]
#[inline]
pub fn pg_checksum_page(page: &Page, blkno: BlockNumber) -> u16 {
    page.checksum(blkno)
}
