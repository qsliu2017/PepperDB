//! Free space map for quickly finding free pages in index relations. Translated from backend/storage/freespace/indexfsm.c.
//!
//! This is similar to the FSM used for the heap, but instead of tracking the
//! amount of free space on each page, an index only needs to know whether a page
//! is completely free or in use. It therefore reuses the heap FSM implementation
//! with two fixed categories: `0` denotes a used page, and `BLCKSZ - 1` (a full
//! empty page) denotes a free one. A request for `BLCKSZ / 2` consequently
//! matches only a wholly-free page. The exported routines are thin wrappers that
//! call into the heap FSM with these category values.
//!
//! Where PostgreSQL passes a catalog `Relation`, these routines take the
//! storage-manager inputs the FSM actually needs: the shared backend state, the
//! open `SmgrRelation`, and the relation's persistence. The operations are async
//! because the underlying FSM reads and writes pages through asynchronous storage
//! I/O.

use std::sync::Arc;

use crate::common::relpath::ForkNumber;
use crate::pg_config::BLCKSZ;
use crate::shared_state::SharedState;
use crate::storage::block::BlockNumber;
use crate::storage::smgr::SmgrRelation;

use super::freespace::{get_page_with_free_space, record_page_with_free_space};

/// C: `GetFreeIndexPage`. Return a wholly-free page from the FSM and mark it
/// used, or `None` if none is free.
pub async fn get_free_index_page(
    shared: &Arc<SharedState>,
    smgr: &mut SmgrRelation,
    relpersistence: i8,
) -> Option<BlockNumber> {
    let blkno = get_page_with_free_space(shared, smgr, relpersistence, BLCKSZ as usize / 2).await;
    if let Some(blkno) = blkno {
        record_used_index_page(shared, smgr, relpersistence, blkno).await;
    }
    blkno
}

/// C: `RecordFreeIndexPage`. Mark a page as free (a full empty page).
pub async fn record_free_index_page(
    shared: &Arc<SharedState>,
    smgr: &mut SmgrRelation,
    relpersistence: i8,
    free_block: BlockNumber,
) {
    record_page_with_free_space(shared, smgr, relpersistence, free_block, BLCKSZ as usize - 1).await;
}

/// C: `RecordUsedIndexPage`. Mark a page as used (no free space).
pub async fn record_used_index_page(
    shared: &Arc<SharedState>,
    smgr: &mut SmgrRelation,
    relpersistence: i8,
    used_block: BlockNumber,
) {
    record_page_with_free_space(shared, smgr, relpersistence, used_block, 0).await;
}

/// C: `IndexFreeSpaceMapVacuum`. Recompute upper-level FSM pages from the leaves.
pub async fn index_free_space_map_vacuum(
    shared: &Arc<SharedState>,
    smgr: &mut SmgrRelation,
    relpersistence: i8,
) {
    super::freespace::free_space_map_vacuum(shared, smgr, relpersistence).await;
}

/// The fork the index FSM lives in (same as the heap FSM fork).
pub const INDEX_FSM_FORK: ForkNumber = ForkNumber::FSM_FORKNUM;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::storage::buffer::buf_init::with_private_refcount;
    use crate::catalog::pg_class::RELPERSISTENCE_PERMANENT;
    use crate::postgres_ext::Oid;
    use crate::shared_state::{SharedState, SharedStateConfig};
    use crate::storage::procnumber::INVALID_PROC_NUMBER;
    use crate::storage::relfilelocator::RelFileLocator;

    fn rloc(rel: u32) -> RelFileLocator {
        RelFileLocator { spcOid: Oid(1663), dbOid: Oid(71000 + rel), relNumber: Oid(19000 + rel) }
    }

    async fn setup(tag: &str, rel: u32) -> (Arc<SharedState>, std::path::PathBuf, SmgrRelation) {
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "pepperdb_indexfsm_{tag}_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        crate::storage::io_backend::mkdir_all(dir.join("base").join((71000 + rel).to_string()))
            .await
            .ok();
        let s = SharedState::new(SharedStateConfig { nbuffers: 64, ..SharedStateConfig::default() });
        s.config().set_data_dir(dir.to_string_lossy().into_owned());
        let mut reln = SmgrRelation::open(rloc(rel), INVALID_PROC_NUMBER);
        reln.create(&s, ForkNumber::MAIN_FORKNUM, false).await;
        (s, dir, reln)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn record_free_then_get_back() {
        let (s, dir, mut reln) = setup("getback", 1).await;
        with_private_refcount(|| async {
            let p = RELPERSISTENCE_PERMANENT;
            record_free_index_page(&s, &mut reln, p, 9).await;
            // RecordFree sets only the bottom leaf; vacuum makes it visible to a
            // search from the root (C semantics).
            index_free_space_map_vacuum(&s, &mut reln, p).await;
            // Get a free page back -> block 9, now marked used.
            let got = get_free_index_page(&s, &mut reln, p).await;
            assert_eq!(got, Some(9));
            // It is now used: a second request (post another vacuum) finds none.
            index_free_space_map_vacuum(&s, &mut reln, p).await;
            let again = get_free_index_page(&s, &mut reln, p).await;
            assert_eq!(again, None);
        })
        .await;
        let _ = crate::storage::io_backend::remove_dir_all(&dir).await;
    }
}
