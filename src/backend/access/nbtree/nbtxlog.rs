//! B-tree WAL replay. Translated from backend/access/nbtree/nbtxlog.c.
//!
//! `btree_redo` re-applies a btree WAL record to the page(s) it references during
//! crash recovery. In PepperDB the btree insert/split/newroot emitters register
//! their modified pages with `REGBUF_FORCE_IMAGE` and do not additionally log the
//! new index tuple as block data (see `nbtinsert::emit_*_wal`). Every such record
//! is therefore self-contained: replay rests on the full-page image, which
//! [`xlog_read_buffer_for_redo`] restores while stamping the page LSN, reproducing
//! the page state exactly (`BLK_RESTORED`). A record whose referenced page already
//! carries the change (LSN guard) is a no-op. The force-image is required because
//! without a checkpointer the redo pointer is invalid, so the LSN-based FPI
//! heuristic alone would omit the image on a second insert into an existing page.
//!
//! INSERT_LEAF/INSERT_UPPER, SPLIT_L/SPLIT_R and NEWROOT are handled through the
//! image-restore path. The rarer variants (dedup, delete, vacuum, page
//! unlink/half-dead, reuse, meta cleanup) are staged: reaching one raises a
//! catchable error rather than silently dropping a change.

use std::sync::Arc;

use crate::access::nbtxlog::{
    XLOG_BTREE_DEDUP, XLOG_BTREE_DELETE, XLOG_BTREE_INSERT_LEAF, XLOG_BTREE_INSERT_META,
    XLOG_BTREE_INSERT_POST, XLOG_BTREE_INSERT_UPPER, XLOG_BTREE_MARK_PAGE_HALFDEAD,
    XLOG_BTREE_META_CLEANUP, XLOG_BTREE_NEWROOT, XLOG_BTREE_REUSE_PAGE, XLOG_BTREE_SPLIT_L,
    XLOG_BTREE_SPLIT_R, XLOG_BTREE_UNLINK_PAGE, XLOG_BTREE_UNLINK_PAGE_META, XLOG_BTREE_VACUUM,
};
use crate::access::xlogreader::DecodedXLogRecord;
use crate::backend::access::transam::xlogutils::{xlog_read_buffer_for_redo, XLogRedoAction};
use crate::shared_state::SharedState;

/// PG `btree_redo`: dispatch a RM_BTREE record to its per-opcode apply routine.
pub async fn btree_redo(shared: &Arc<SharedState>, record: &DecodedXLogRecord) {
    let info = record.info() & 0xF0;
    match info {
        XLOG_BTREE_INSERT_LEAF | XLOG_BTREE_INSERT_UPPER | XLOG_BTREE_INSERT_META
        | XLOG_BTREE_INSERT_POST => btree_xlog_insert(shared, record).await,
        XLOG_BTREE_SPLIT_L | XLOG_BTREE_SPLIT_R => btree_xlog_split(shared, record).await,
        XLOG_BTREE_NEWROOT => btree_xlog_newroot(shared, record).await,
        XLOG_BTREE_DEDUP
        | XLOG_BTREE_DELETE
        | XLOG_BTREE_VACUUM
        | XLOG_BTREE_MARK_PAGE_HALFDEAD
        | XLOG_BTREE_UNLINK_PAGE
        | XLOG_BTREE_UNLINK_PAGE_META
        | XLOG_BTREE_REUSE_PAGE
        | XLOG_BTREE_META_CLEANUP => crate::elog!(
            crate::utils::elog::ERROR,
            format!("btree_redo: unimplemented btree opcode {info:#x} (staged)")
        ),
        other => crate::elog!(
            crate::utils::elog::ERROR,
            format!("btree_redo: unknown btree opcode {other:#x}")
        ),
    }
}

/// PG `btree_xlog_insert`: re-apply a leaf/upper index-tuple insert. The write
/// path forces a full-page image (REGBUF_FORCE_IMAGE), so the image restore
/// reproduces the insert (BLK_RESTORED). BLK_NEEDS_REDO here would mean a record
/// reached redo without its image, which the write path cannot emit -- flagged
/// rather than silently skipped.
async fn btree_xlog_insert(shared: &Arc<SharedState>, record: &DecodedXLogRecord) {
    let rb = xlog_read_buffer_for_redo(shared, record, 0).await;
    needs_image_or_stage(rb.action, "insert");
    release_if_valid(shared, rb.buffer);
}

/// PG `btree_xlog_split`: the left and right pages are both `WILL_INIT` and thus
/// fully imaged; restoring both images reproduces the split.
async fn btree_xlog_split(shared: &Arc<SharedState>, record: &DecodedXLogRecord) {
    // Left page (block 0) then right page (block 1); each is image-restored.
    let rb_left = xlog_read_buffer_for_redo(shared, record, 0).await;
    needs_image_or_stage(rb_left.action, "btree split left");
    release_if_valid(shared, rb_left.buffer);

    let rb_right = xlog_read_buffer_for_redo(shared, record, 1).await;
    needs_image_or_stage(rb_right.action, "btree split right");
    release_if_valid(shared, rb_right.buffer);
}

/// PG `btree_xlog_newroot`: the new root page is `WILL_INIT` and imaged.
async fn btree_xlog_newroot(shared: &Arc<SharedState>, record: &DecodedXLogRecord) {
    let rb = xlog_read_buffer_for_redo(shared, record, 0).await;
    needs_image_or_stage(rb.action, "btree newroot");
    release_if_valid(shared, rb.buffer);
}

/// A `WILL_INIT` page is expected to be restored from its image (or already
/// current via the LSN guard). BLK_NEEDS_REDO with no image means the incremental
/// apply path is required, which is staged.
fn needs_image_or_stage(action: XLogRedoAction, what: &str) {
    if action == XLogRedoAction::BLK_NEEDS_REDO {
        crate::elog!(
            crate::utils::elog::ERROR,
            format!("btree_redo {what}: page needs redo but no full-page image (staged)")
        );
    }
}

fn release_if_valid(shared: &Arc<SharedState>, buffer: crate::storage::buf::Buffer) {
    if buffer != crate::storage::buf::INVALID_BUFFER {
        shared.buffers().release_buffer(buffer);
    }
}
