//! WAL-replay buffer utilities. Translated from backend/access/transam/xlogutils.c.
//!
//! The redo side of the WAL machinery: given a decoded record and one of its
//! block references, obtain the shared buffer that block lives in and decide
//! whether the record's change still needs to be applied. The central rule is the
//! LSN guard -- if the page's LSN is already at or past the record's end LSN, the
//! change is on the page and replay is a no-op ([`XLogRedoAction::BLK_DONE`]);
//! otherwise the caller applies the change ([`XLogRedoAction::BLK_NEEDS_REDO`]).
//! When a record carries a full-page image for the block, the image is restored
//! verbatim, the page LSN is stamped, and the caller is told the block is already
//! current ([`XLogRedoAction::BLK_RESTORED`]).
//!
//! In PepperDB these are async methods taking `&Arc<SharedState>` and the decoded
//! record, replacing PG's `XLogReaderState*` out-param `Buffer*` signatures. The
//! returned [`RedoBuffer`] carries the pinned buffer id so the caller can mutate
//! the page under the content-exclusive lock, mark it dirty, and release it. No
//! buffer-manager or smgr lock is held across an `.await`.

use std::sync::Arc;

use crate::access::xlogdefs::XLogRecPtr;
use crate::access::xlogreader::DecodedXLogRecord;
use crate::storage::smgr::SmgrRelation;
use crate::common::relpath::ForkNumber;
use crate::storage::block::{BlockNumber, INVALID_BLOCK_NUMBER};
use crate::storage::buf::{Buffer, INVALID_BUFFER};
use crate::storage::bufmgr::ReadBufferMode;
use crate::storage::procnumber::INVALID_PROC_NUMBER;
use crate::backend::storage::buffer::bufmgr::read_buffer_common;
use crate::catalog::pg_class::RELPERSISTENCE_PERMANENT;
use crate::shared_state::SharedState;
use crate::utils::elog::OrElog;

/// Result codes for [`xlog_read_buffer_for_redo`] (PG `XLogRedoAction`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(non_camel_case_types, reason = "1:1 name of PG's XLogRedoAction enum")]
pub enum XLogRedoAction {
    /// The WAL record's changes need to be applied to the page.
    BLK_NEEDS_REDO,
    /// The page is already up-to-date (LSN guard tripped); nothing to do.
    BLK_DONE,
    /// The page was restored from a full-page image; nothing more to do.
    BLK_RESTORED,
    /// The block was not found (relation/fork removed); need not be replayed.
    BLK_NOTFOUND,
}

/// A pinned buffer returned by the redo buffer helpers, plus the action the
/// caller must take. Mirrors the C `Buffer *buf` out-param folded with the
/// returned `XLogRedoAction`.
#[derive(Debug, Clone, Copy)]
pub struct RedoBuffer {
    pub action: XLogRedoAction,
    pub buffer: Buffer,
}

/// PG `XLogReadBufferForRedo`: the common redo path -- read block `block_id`'s
/// buffer, restoring a full-page image if the record carries one, else applying
/// the LSN guard. Equivalent to `XLogReadBufferForRedoExtended(.., NORMAL,
/// false)`.
pub async fn xlog_read_buffer_for_redo(
    shared: &Arc<SharedState>,
    record: &DecodedXLogRecord,
    block_id: u8,
) -> RedoBuffer {
    xlog_read_buffer_for_redo_extended(shared, record, block_id, ReadBufferMode::NORMAL, false).await
}

/// PG `XLogInitBufferForRedo`: get the buffer for a block that the record
/// re-initializes from scratch (the caller `PageInit`s it). Zeroes/pins the page
/// with no LSN guard (the page content is about to be replaced).
pub async fn xlog_init_buffer_for_redo(
    shared: &Arc<SharedState>,
    record: &DecodedXLogRecord,
    block_id: u8,
) -> RedoBuffer {
    xlog_read_buffer_for_redo_extended(shared, record, block_id, ReadBufferMode::ZERO_AND_LOCK, true)
        .await
}

/// PG `XLogReadBufferForRedoExtended`: the full redo buffer read.
///
/// Reads (extending the fork with zero pages if the block is past end-of-file,
/// as PG does during redo) the buffer holding `block_id`'s block. Then:
/// - if the record has a full-page image for this block and it is marked
///   `apply`, restore the image, stamp the page LSN, mark dirty -> `BLK_RESTORED`;
/// - else if the page LSN is already `>= record end LSN`, the change is present
///   -> `BLK_DONE`;
/// - else the caller must apply the change -> `BLK_NEEDS_REDO`.
///
/// `will_init` (true for [`xlog_init_buffer_for_redo`]) means the block is
/// re-initialized regardless of its current content, so the read zeroes the page
/// and the LSN guard is skipped.
pub async fn xlog_read_buffer_for_redo_extended(
    shared: &Arc<SharedState>,
    record: &DecodedXLogRecord,
    block_id: u8,
    mode: ReadBufferMode,
    will_init: bool,
) -> RedoBuffer {
    let Some((rlocator, forknum, blkno, _)) = record.get_block_tag_extended(block_id) else {
        return RedoBuffer { action: XLogRedoAction::BLK_NOTFOUND, buffer: INVALID_BUFFER };
    };

    // The record's end LSN is the LSN the page must reach; a page already at/past
    // it has the change (idempotent replay).
    let lsn = record.next_lsn;
    let has_image = record.has_block_image_for(block_id);

    // When the record carries an image (and it is to be applied), the page is
    // fully rebuilt from it; zero-and-lock so no read from disk is needed.
    let zero_mode = if has_image && !will_init {
        ReadBufferMode::ZERO_AND_LOCK
    } else {
        mode
    };

    let buffer =
        xlog_read_buffer_extended(shared, rlocator, forknum, blkno, zero_mode).await;
    if buffer == INVALID_BUFFER {
        return RedoBuffer { action: XLogRedoAction::BLK_NOTFOUND, buffer };
    }

    let pool = shared.buffers();
    let buf_id = buffer.as_global().unwrap_or_panic_with(|| "redo: shared buffer expected") as i32;

    if has_image {
        // Restore the full-page image verbatim, stamp the LSN, mark dirty.
        let guard = pool.content_exclusive(buffer);
        // SAFETY: content-exclusive lock held; this is the only writer.
        let page = unsafe { pool.block_mut(buf_id) };
        record
            .restore_block_image(block_id, page.as_mut_bytes())
            .unwrap_or_panic_with(|| "redo: could not restore full-page image");
        page.set_lsn(lsn);
        drop(guard);
        pool.mark_buffer_dirty(buffer);
        return RedoBuffer { action: XLogRedoAction::BLK_RESTORED, buffer };
    }

    if will_init {
        // Re-initialized page: the caller rebuilds it; skip the LSN guard.
        return RedoBuffer { action: XLogRedoAction::BLK_NEEDS_REDO, buffer };
    }

    // LSN guard: if the page is already at/past the record end LSN, the change is
    // present -> nothing to do. Read the LSN under a shared content lock.
    let page_lsn = {
        let _g = pool.content_share(buffer);
        pool.buffer_get_page(buffer).get_lsn()
    };
    let action = if page_lsn >= lsn {
        XLogRedoAction::BLK_DONE
    } else {
        XLogRedoAction::BLK_NEEDS_REDO
    };
    RedoBuffer { action, buffer }
}

/// PG `XLogReadBufferExtended`: read (and, during redo, extend to reach) the
/// block. If the requested block is past the fork's current end, the fork is
/// extended with zero pages up to it (a WAL record can legitimately reference a
/// block a prior, lost extend would have created). Returns [`INVALID_BUFFER`] if
/// the relation/fork does not exist.
pub async fn xlog_read_buffer_extended(
    shared: &Arc<SharedState>,
    rlocator: crate::storage::relfilelocator::RelFileLocator,
    forknum: ForkNumber,
    blkno: BlockNumber,
    mode: ReadBufferMode,
) -> Buffer {
    let mut smgr = SmgrRelation::open(rlocator, INVALID_PROC_NUMBER);
    let last_block = smgr.nblocks(shared, forknum).await;

    if blkno < last_block {
        return read_buffer_common(
            shared,
            &mut smgr,
            RELPERSISTENCE_PERMANENT,
            forknum,
            blkno,
            mode,
            None,
        )
        .await;
    }

    // The block is at or past EOF: extend the fork with zero pages until the
    // target block exists, then read it. PG loops ReadBufferExtended(P_NEW).
    debug_assert_ne!(last_block, INVALID_BLOCK_NUMBER);
    let mut buffer = INVALID_BUFFER;
    let mut cur = last_block;
    while cur <= blkno {
        if buffer != INVALID_BUFFER {
            shared.buffers().release_buffer(buffer);
        }
        buffer = read_buffer_common(
            shared,
            &mut smgr,
            RELPERSISTENCE_PERMANENT,
            forknum,
            crate::storage::bufmgr::P_NEW,
            mode,
            None,
        )
        .await;
        cur = shared.buffers().buffer_get_block_number(buffer);
        cur += 1;
    }
    debug_assert_eq!(shared.buffers().buffer_get_block_number(buffer), blkno);
    buffer
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::rmgrlist::RmgrId;
    use crate::backend::access::transam::xlog::XLogCtl;
    use crate::backend::access::transam::xloginsert::{
        begin_insert, log_newpage, register_block, register_buf_data, register_data, with_insertion,
        xlog_insert,
    };
    use crate::access::xloginsert::RegBuf;
    use crate::backend::access::transam::xlogreader::XLogReader;
    use crate::shared_state::{SharedState, SharedStateConfig};
    use crate::storage::bufpage::Page;
    use crate::storage::relfilelocator::RelFileLocator;
    use crate::storage::smgr::SmgrRelation;
    use std::sync::atomic::{AtomicU32, Ordering};

    static N: AtomicU32 = AtomicU32::new(0);

    fn shared_on_tmp() -> (Arc<SharedState>, std::path::PathBuf) {
        let n = N.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("pepperdb-xlogutils-{}-{}", std::process::id(), n));
        let _ = std::fs::create_dir_all(dir.join(crate::access::xlog_internal::XLOGDIR));
        let _ = std::fs::create_dir_all(dir.join("base").join("90000"));
        let s = SharedState::new(SharedStateConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            nbuffers: 32,
            ..Default::default()
        });
        (s, dir)
    }

    fn rloc(rel: u32) -> RelFileLocator {
        RelFileLocator {
            spcOid: crate::postgres_ext::Oid::new(1663),
            dbOid: crate::postgres_ext::Oid::new(90000),
            relNumber: crate::postgres_ext::Oid::new(70000 + rel),
        }
    }

    /// Read the (only) record starting at `start` from the on-disk WAL.
    fn read_one(
        xlog: &Arc<XLogCtl>,
        start: XLogRecPtr,
    ) -> crate::access::xlogreader::DecodedXLogRecord {
        let page_read = xlog.make_recovery_page_reader().expect("page reader");
        let mut reader = XLogReader::new(xlog.wal_segment_size(), page_read);
        reader.set_system_identifier(0);
        reader.begin_read(start);
        reader.read_record().unwrap().expect("a record").clone()
    }

    /// A full-page-image record replays via BLK_RESTORED and reproduces the page.
    #[tokio::test(flavor = "multi_thread")]
    async fn fpi_record_restores_page() {
        let (shared, dir) = shared_on_tmp();
        let loc = rloc(1);
        // Create the fork so the redo buffer read can extend/find it.
        let mut smgr = SmgrRelation::open(loc, crate::storage::procnumber::INVALID_PROC_NUMBER);
        smgr.create(&shared, ForkNumber::MAIN_FORKNUM, false).await;

        // Build a recognizable standard page and log it as an FPI.
        let mut page = Page::zeroed();
        page.as_mut_bytes()[12..14].copy_from_slice(&40u16.to_ne_bytes()); // pd_lower
        page.as_mut_bytes()[14..16].copy_from_slice(&8000u16.to_ne_bytes()); // pd_upper
        for i in 24..40 {
            page.as_mut_bytes()[i] = (i as u8).wrapping_mul(3);
        }
        let expected = page.as_bytes().to_vec();

        let start = shared.xlog().get_xlog_insert_rec_ptr();
        let end = with_insertion(async {
            log_newpage(shared.xlog(), &loc, ForkNumber::MAIN_FORKNUM, 0, &page, true).await
        })
        .await;
        shared.xlog().xlog_flush(end).await;

        let rec = read_one(shared.xlog(), start);
        let rb = xlog_read_buffer_for_redo(&shared, &rec, 0).await;
        assert_eq!(rb.action, XLogRedoAction::BLK_RESTORED, "FPI must restore the page");

        // The restored buffer page equals the original (LSN aside: the redo path
        // stamps the record's end LSN into the first 8 bytes).
        let pool = shared.buffers();
        let got = {
            let _g = pool.content_share(rb.buffer);
            pool.buffer_get_page(rb.buffer).as_bytes().to_vec()
        };
        pool.release_buffer(rb.buffer);
        assert_eq!(&got[8..], &expected[8..], "restored page content matches original");
        assert_eq!(pool.buffer_get_page(rb.buffer).get_lsn(), rec.next_lsn);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The LSN guard: a record whose page LSN already meets/exceeds the record end
    /// LSN is skipped (BLK_DONE). Built by writing a data-only (no image) record,
    /// then pre-stamping the page LSN past the record's end LSN.
    #[tokio::test(flavor = "multi_thread")]
    async fn lsn_guard_skips_already_applied() {
        let (shared, dir) = shared_on_tmp();
        let loc = rloc(2);
        let mut smgr = SmgrRelation::open(loc, crate::storage::procnumber::INVALID_PROC_NUMBER);
        smgr.create(&shared, ForkNumber::MAIN_FORKNUM, false).await;
        // Extend the fork to have block 0.
        let b0 = read_buffer_common(
            &shared, &mut smgr, RELPERSISTENCE_PERMANENT, ForkNumber::MAIN_FORKNUM,
            crate::storage::bufmgr::P_NEW, ReadBufferMode::NORMAL, None,
        )
        .await;
        shared.buffers().release_buffer(b0);

        // A block-0 record with per-block data but NO image (NO_IMAGE).
        let page = Page::zeroed();
        let start = shared.xlog().get_xlog_insert_rec_ptr();
        let end = with_insertion(async {
            begin_insert();
            register_data(b"main");
            register_block(0, &loc, ForkNumber::MAIN_FORKNUM, 0, &page, RegBuf::NO_IMAGE);
            register_buf_data(0, b"blockdata");
            xlog_insert(shared.xlog(), RmgrId::Heap as u8, 0x00).await
        })
        .await;
        shared.xlog().xlog_flush(end).await;

        let rec = read_one(shared.xlog(), start);

        // Pre-stamp block 0's page LSN to the record's end LSN (as if the change
        // were already applied). Then the guard must report BLK_DONE.
        {
            let buffer = xlog_read_buffer_extended(
                &shared, loc, ForkNumber::MAIN_FORKNUM, 0, ReadBufferMode::NORMAL,
            )
            .await;
            let pool = shared.buffers();
            let buf_id = buffer.as_global().unwrap() as i32;
            {
                let _g = pool.content_exclusive(buffer);
                // SAFETY: exclusive content lock held.
                unsafe { pool.block_mut(buf_id) }.set_lsn(rec.next_lsn);
            }
            pool.mark_buffer_dirty(buffer);
            pool.release_buffer(buffer);
        }

        let rb = xlog_read_buffer_for_redo(&shared, &rec, 0).await;
        assert_eq!(rb.action, XLogRedoAction::BLK_DONE, "LSN guard must skip an already-applied change");
        shared.buffers().release_buffer(rb.buffer);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
