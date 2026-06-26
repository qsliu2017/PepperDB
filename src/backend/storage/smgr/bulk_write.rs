//! Translated from PostgreSQL src/backend/storage/smgr/bulk_write.c
//!
//! Efficiently populate a new relation by bypassing the buffer manager and
//! calling smgr extend/write directly, batching WAL records. Pages are buffered
//! and flushed in batches (sorted by block number); the batch is WAL-logged via
//! [`log_newpages`] (the step-13 xloginsert stub) when `use_wal`, then written.
//! Because we bypass the buffer manager we register the relation for fsync at
//! finish.
//!
//! Port notes: `BulkWriteBuffer` is an owned `Box<Page>` (smgr.c's palloc'd
//! aligned block); `smgr_bulk_get_buf` returns a fresh zeroed page and
//! `smgr_bulk_write` takes ownership. The MyProc->delayChkptFlags / RedoRecPtr
//! checkpoint-race guard is TODO(step13/step17) -- we always registersync.

use std::sync::Arc;

use crate::access::xloginsert::log_newpages;
use crate::common::relpath::ForkNumber;
use crate::shared_state::SharedState;
use crate::storage::block::BlockNumber;
use crate::storage::bufpage::Page;
use crate::storage::smgr::SmgrRelation;

/// Max pages buffered before an automatic flush (smgr.c MAX_PENDING_WRITES =
/// XLR_MAX_BLOCK_ID).
const MAX_PENDING_WRITES: usize = crate::access::xlogrecord::XLR_MAX_BLOCK_ID as usize;

/// A page-sized buffer handed out by [`BulkWriteState::get_buf`].
pub type BulkWriteBuffer = Box<Page>;

struct PendingWrite {
    buf: BulkWriteBuffer,
    blkno: BlockNumber,
    page_std: bool,
}

/// Bulk writer state for one relation fork (smgr.c `BulkWriteState`). Borrows
/// the target [`SmgrRelation`] mutably for the lifetime of the bulk write.
pub struct BulkWriteState<'a> {
    smgr: &'a mut SmgrRelation,
    forknum: ForkNumber,
    use_wal: bool,
    pending: Vec<PendingWrite>,
    relsize: BlockNumber,
    shared: Arc<SharedState>,
}

impl<'a> BulkWriteState<'a> {
    /// smgr_bulk_start_smgr() -- begin a bulk write on `smgr`'s `forknum`.
    pub async fn start_smgr(
        shared: Arc<SharedState>,
        smgr: &'a mut SmgrRelation,
        forknum: ForkNumber,
        use_wal: bool,
    ) -> BulkWriteState<'a> {
        let relsize = smgr.nblocks(&shared, forknum).await;
        // TODO(step13): start_RedoRecPtr = GetRedoRecPtr() for the finish-time
        // concurrent-checkpoint race check.
        BulkWriteState { smgr, forknum, use_wal, pending: Vec::new(), relsize, shared }
    }

    /// smgr_bulk_get_buf() -- a fresh page-sized buffer to fill.
    pub fn get_buf(&self) -> BulkWriteBuffer {
        Page::boxed_zeroed()
    }

    /// smgr_bulk_write() -- queue `buf` for block `blocknum` (takes ownership).
    pub async fn write(&mut self, blocknum: BlockNumber, buf: BulkWriteBuffer, page_std: bool) {
        self.pending.push(PendingWrite { buf, blkno: blocknum, page_std });
        if self.pending.len() == MAX_PENDING_WRITES {
            self.flush().await;
        }
    }

    /// smgr_bulk_flush() -- WAL-log (if needed) and write all pending pages.
    async fn flush(&mut self) {
        if self.pending.is_empty() {
            return;
        }
        // We should not see duplicate blocks; sort by block number.
        self.pending.sort_by_key(|w| w.blkno);

        if self.use_wal {
            let blknos: Vec<BlockNumber> = self.pending.iter().map(|w| w.blkno).collect();
            let pages: Vec<&Page> = self.pending.iter().map(|w| w.buf.as_ref()).collect();
            let page_std = self.pending.iter().all(|w| w.page_std);
            log_newpages(
                self.shared.xlog(),
                &self.smgr.rlocator.locator,
                self.forknum,
                &blknos,
                &pages,
                page_std,
            )
            .await;
        }

        let pending = std::mem::take(&mut self.pending);
        for w in pending {
            // TODO(checksum): PageSetChecksumInplace(page, blkno).
            if w.blkno >= self.relsize {
                // Fill any gap with zero pages (not WAL-logged), then write.
                while w.blkno > self.relsize {
                    let zero = Page::boxed_zeroed();
                    self.smgr
                        .extend(&self.shared, self.forknum, self.relsize, &zero, true)
                        .await;
                    self.relsize += 1;
                }
                self.smgr.extend(&self.shared, self.forknum, w.blkno, &w.buf, true).await;
                self.relsize += 1;
            } else {
                self.smgr.write(&self.shared, self.forknum, w.blkno, &w.buf, true).await;
            }
        }
    }

    /// smgr_bulk_finish() -- flush remaining pages and register the relation for
    /// fsync at the next checkpoint (unless temp).
    pub async fn finish(mut self) {
        self.flush().await;

        if self.smgr.is_temp() {
            // Temp relations are never fsync'd.
        } else {
            // For both the unlogged and WAL-logged cases we registersync here.
            // TODO(step13/step17): the WAL-logged case should check whether a
            // checkpoint started concurrently (RedoRecPtr changed) and call
            // immedsync instead; needs GetRedoRecPtr + delayChkptFlags.
            self.smgr.registersync(&self.shared, self.forknum).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres_ext::Oid;
    use crate::shared_state::SharedStateConfig;
    use crate::storage::relfilelocator::RelFileLocator;

    fn shared_with_tmpdir(tag: &str) -> (Arc<SharedState>, std::path::PathBuf) {
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "pepperdb_bulk_{tag}_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let s = SharedState::new(SharedStateConfig::default());
        s.config().set_data_dir(dir.to_string_lossy().into_owned());
        (s, dir)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn bulk_write_no_wal_path() {
        let (s, dir) = shared_with_tmpdir("nowal");
        let rloc = RelFileLocator { spcOid: Oid(1663), dbOid: Oid(70001), relNumber: Oid(18000) };
        let mut reln = SmgrRelation::open(rloc, crate::storage::procnumber::INVALID_PROC_NUMBER);
        let fork = ForkNumber::MAIN_FORKNUM;
        reln.create(&s, fork, false).await;

        {
            let mut bulk = BulkWriteState::start_smgr(s.clone(), &mut reln, fork, false).await;
            for i in 0..3u32 {
                let mut buf = bulk.get_buf();
                buf.as_mut_bytes().fill(0x30 + i as u8);
                bulk.write(i, buf, true).await;
            }
            bulk.finish().await;
        }

        assert_eq!(reln.nblocks(&s, fork).await, 3);
        // No-WAL finish registers the relation for sync.
        assert!(s.sync_requests().pending_op_count() >= 1);

        // Pages are readable with their patterns.
        for i in 0..3u32 {
            let mut buf = Page::boxed_zeroed();
            reln.read(&s, fork, i, &mut buf).await;
            assert!(buf.as_bytes().iter().all(|&b| b == 0x30 + i as u8));
        }

        let _ = crate::storage::io_backend::remove_dir_all(&dir).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn bulk_write_wal_path() {
        let (s, dir) = shared_with_tmpdir("wal");
        // The WAL pipeline needs pg_wal/ to exist for segment files.
        crate::storage::io_backend::mkdir_all(
            dir.join(crate::access::xlog_internal::XLOGDIR),
        )
        .await
        .unwrap();

        let rloc = RelFileLocator { spcOid: Oid(1663), dbOid: Oid(70002), relNumber: Oid(18001) };
        let mut reln = SmgrRelation::open(rloc, crate::storage::procnumber::INVALID_PROC_NUMBER);
        let fork = ForkNumber::MAIN_FORKNUM;
        reln.create(&s, fork, false).await;

        let before = s.xlog().get_xlog_insert_rec_ptr();
        {
            // use_wal = true exercises the real log_newpages WAL path.
            let mut bulk = BulkWriteState::start_smgr(s.clone(), &mut reln, fork, true).await;
            for i in 0..3u32 {
                let mut buf = bulk.get_buf();
                buf.as_mut_bytes().fill(0x41 + i as u8);
                bulk.write(i, buf, true).await;
            }
            bulk.finish().await;
        }

        // The WAL insert head advanced (an FPI record was logged).
        let after = s.xlog().get_xlog_insert_rec_ptr();
        assert!(after.0 > before.0, "WAL should have advanced for the FPI record");

        // Pages are still written and readable.
        assert_eq!(reln.nblocks(&s, fork).await, 3);
        for i in 0..3u32 {
            let mut buf = Page::boxed_zeroed();
            reln.read(&s, fork, i, &mut buf).await;
            assert!(buf.as_bytes().iter().all(|&b| b == 0x41 + i as u8));
        }

        let _ = crate::storage::io_backend::remove_dir_all(&dir).await;
    }
}
