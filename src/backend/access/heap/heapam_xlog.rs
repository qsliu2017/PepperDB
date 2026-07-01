//! Heap WAL replay. Translated from backend/access/heap/heapam_xlog.c.
//!
//! `heap_redo` re-applies the changes a heap WAL record describes to the page it
//! references, during crash recovery. For every record the page it touches is
//! obtained through the redo buffer helper, which either restores a full-page
//! image (leaving nothing to do), reports the change already present (the LSN
//! guard), or hands back a page that still needs the change applied. When the
//! change is applied here, the page LSN is stamped with the record's end LSN so a
//! re-run of the same record is a no-op.
//!
//! INSERT, DELETE, UPDATE and LOCK (the paths a running system exercises for
//! ordinary DML + row locks) are implemented -- every heap opcode the write path
//! emits is replayable. MULTI_INSERT and the `heap2` variants (prune, vacuum,
//! visible, freeze) are staged: their apply logic is not ported, so reaching one
//! during replay raises a catchable error rather than silently skipping a change.
//!
//! The tuple a redo record carries is a reduced form (`xl_heap_header`: the
//! infomask fields + `t_hoff`, then the bitmap+data body); the full 23-byte
//! on-disk header is rebuilt here from that plus the record's inserting XID, as
//! `heap_xlog_insert` does in C.

use std::sync::Arc;

use crate::access::heapam_xlog::{
    xl_heap_delete, xl_heap_header, xl_heap_insert, xl_heap_lock, xl_heap_update, SizeOfHeapDelete,
    SizeOfHeapHeader, SizeOfHeapInsert, SizeOfHeapLock, SizeOfHeapUpdate,
    XLH_UPDATE_CONTAINS_NEW_TUPLE, XLOG_HEAP_DELETE, XLOG_HEAP_INIT_PAGE, XLOG_HEAP_INSERT,
    XLOG_HEAP_LOCK, XLOG_HEAP_OPMASK, XLOG_HEAP_UPDATE,
};
use crate::access::htup_details::{
    SizeofHeapTupleHeader, HEAP_COMBOCID, HEAP_HOT_UPDATED, HEAP_KEYS_UPDATED, HEAP_MOVED,
    HEAP_XMAX_BITS, HEAP_XMAX_EXCL_LOCK, HEAP_XMAX_IS_LOCKED_ONLY, HEAP_XMAX_IS_MULTI,
    HEAP_XMAX_KEYSHR_LOCK, HEAP_XMAX_LOCK_ONLY,
};
use crate::access::heapam_xlog::{
    XLHL_KEYS_UPDATED, XLHL_XMAX_EXCL_LOCK, XLHL_XMAX_IS_MULTI, XLHL_XMAX_KEYSHR_LOCK,
    XLHL_XMAX_LOCK_ONLY,
};
use crate::access::xlogreader::DecodedXLogRecord;
use crate::backend::access::transam::xlogutils::{
    xlog_init_buffer_for_redo, xlog_read_buffer_for_redo, XLogRedoAction,
};
use crate::c::TransactionId;
use crate::pg_config::BLCKSZ;
use crate::storage::bufpage::PageAddItemFlags;
use crate::storage::off::OffsetNumber;
use crate::shared_state::SharedState;
use crate::utils::elog::OrElog;

const FIRST_COMMAND_ID: u32 = 0;

/// PG `heap_redo`: dispatch a RM_HEAP record to its per-opcode apply routine.
pub async fn heap_redo(shared: &Arc<SharedState>, record: &DecodedXLogRecord) {
    let info = record.info();
    match info & XLOG_HEAP_OPMASK {
        XLOG_HEAP_INSERT => heap_xlog_insert(shared, record).await,
        XLOG_HEAP_DELETE => heap_xlog_delete(shared, record).await,
        XLOG_HEAP_UPDATE => heap_xlog_update(shared, record, false).await,
        XLOG_HEAP_LOCK => heap_xlog_lock(shared, record).await,
        // HOT update shares the update path; other opcodes are staged.
        other => crate::elog!(
            crate::utils::elog::ERROR,
            format!("heap_redo: unimplemented heap opcode {other:#x} (staged)")
        ),
    }
}

/// PG `heap2_redo`: RM_HEAP2 records (multi-insert, prune, vacuum, visible,
/// freeze). Staged -- the apply logic is not yet ported.
#[allow(
    clippy::unused_async,
    reason = "async to match the redo dispatch signature; body diverges (staged)"
)]
pub async fn heap2_redo(_shared: &Arc<SharedState>, record: &DecodedXLogRecord) {
    let info = record.info();
    crate::elog!(
        crate::utils::elog::ERROR,
        format!(
            "heap2_redo: unimplemented heap2 opcode {:#x} (staged)",
            info & XLOG_HEAP_OPMASK
        )
    );
}

/// Build the on-disk 23-byte heap tuple header + body from a WAL `xl_heap_header`
/// and the tuple body bytes, filling xmin from the record's XID (cmin =
/// FirstCommandId, xmax invalid) and self-referencing ctid. Mirrors the tuple
/// reconstruction in C `heap_xlog_insert`.
fn build_redo_tuple(
    xlhdr: &xl_heap_header,
    body: &[u8],
    xid: TransactionId,
    blkno: u32,
    offnum: OffsetNumber,
) -> Vec<u8> {
    let mut tup = vec![0u8; SizeofHeapTupleHeader + body.len()];
    // choice.t_heap.xmin (0..4); xmax (4..8) left 0; field3 = cmin (8..12).
    tup[0..4].copy_from_slice(&xid.0.to_ne_bytes());
    tup[8..12].copy_from_slice(&FIRST_COMMAND_ID.to_ne_bytes());
    // ctid = (blkno, offnum): block (12..16), offset (16..18).
    tup[12..16].copy_from_slice(&blkno.to_ne_bytes());
    tup[16..18].copy_from_slice(&offnum.to_ne_bytes());
    // t_infomask2 (18..20), t_infomask (20..22), t_hoff (22).
    tup[18..20].copy_from_slice(&xlhdr.t_infomask2.to_ne_bytes());
    tup[20..22].copy_from_slice(&xlhdr.t_infomask.to_ne_bytes());
    tup[22] = xlhdr.t_hoff;
    // Body (nulls bitmap + padding + user data) after the fixed header.
    tup[SizeofHeapTupleHeader..].copy_from_slice(body);
    tup
}

/// Read an `xl_heap_header` from the front of a block-data slice.
fn read_xl_heap_header(data: &[u8]) -> xl_heap_header {
    xl_heap_header {
        t_infomask2: u16::from_ne_bytes([data[0], data[1]]),
        t_infomask: u16::from_ne_bytes([data[2], data[3]]),
        t_hoff: data[4],
    }
}

/// PG `heap_xlog_insert`: re-insert a tuple at its recorded offset.
async fn heap_xlog_insert(shared: &Arc<SharedState>, record: &DecodedXLogRecord) {
    let main = record.get_data().unwrap_or_panic_with(|| "heap insert redo: missing main data");
    assert!(main.len() >= SizeOfHeapInsert);
    let offnum = u16::from_ne_bytes([main[0], main[1]]);
    // main[2] = flags (XlhInsert); ALL_FROZEN_SET not used on this path.

    let (_, _, blkno) = record.get_block_tag(0);
    let xid = record.header.xid;

    // INIT_PAGE re-initializes the page from scratch; else read + LSN guard.
    let init_page = record.info() & XLOG_HEAP_INIT_PAGE != 0;
    let rb = if init_page {
        xlog_init_buffer_for_redo(shared, record, 0).await
    } else {
        xlog_read_buffer_for_redo(shared, record, 0).await
    };

    if rb.action == XLogRedoAction::BLK_NEEDS_REDO {
        let pool = shared.buffers();
        let buf_id = rb.buffer.as_global().unwrap_or_panic_with(|| "redo: shared buffer expected") as i32;
        let block_data = record.get_block_data(0).unwrap_or_panic_with(|| "heap insert redo: missing block data").to_vec();
        assert!(block_data.len() > SizeOfHeapHeader);
        let xlhdr = read_xl_heap_header(&block_data);
        let body = &block_data[SizeOfHeapHeader..];
        let tuple = build_redo_tuple(&xlhdr, body, xid, blkno, offnum);

        let guard = pool.content_exclusive(rb.buffer);
        // SAFETY: content-exclusive lock held; sole writer.
        let page = unsafe { pool.block_mut(buf_id) };
        if init_page {
            page.init(BLCKSZ as usize, 0);
        }
        assert!(
            u32::from(page.get_max_offset_number()) + 1 >= u32::from(offnum),
            "heap insert redo: invalid max offset"
        );
        let added = page.add_item_extended(
            &tuple,
            tuple.len(),
            offnum,
            PageAddItemFlags::OVERWRITE | PageAddItemFlags::IS_HEAP,
        );
        assert_ne!(added, 0, "heap insert redo: failed to add tuple");
        page.set_lsn(record.next_lsn);
        drop(guard);
        pool.mark_buffer_dirty(rb.buffer);
    }
    release_if_valid(shared, rb.buffer);
}

/// PG `heap_xlog_delete`: stamp the deleted tuple's xmax + infomask in place.
async fn heap_xlog_delete(shared: &Arc<SharedState>, record: &DecodedXLogRecord) {
    let main = record.get_data().unwrap_or_panic_with(|| "heap delete redo: missing main data");
    assert!(main.len() >= SizeOfHeapDelete);
    let xlrec = xl_heap_delete {
        xmax: TransactionId(u32::from_ne_bytes([main[0], main[1], main[2], main[3]])),
        offnum: u16::from_ne_bytes([main[4], main[5]]),
        infobits_set: main[6],
        flags: main[7],
    };

    let rb = xlog_read_buffer_for_redo(shared, record, 0).await;
    if rb.action == XLogRedoAction::BLK_NEEDS_REDO {
        let pool = shared.buffers();
        let buf_id = rb.buffer.as_global().unwrap_or_panic_with(|| "redo: shared buffer expected") as i32;
        let guard = pool.content_exclusive(rb.buffer);
        // SAFETY: content-exclusive lock held; sole writer.
        let page = unsafe { pool.block_mut(buf_id) };

        let item_id = page.get_item_id(xlrec.offnum);
        let tuple_off = item_id_offset(item_id);
        let (_, _, blkno) = record.get_block_tag(0);
        let bytes = page.as_mut_bytes();
        // Stamp xmax + cmax + infobits, clearing HOT-updated (ClearHotUpdated).
        stamp_xmax_infobits(bytes, tuple_off, xlrec.xmax, xlrec.infobits_set);
        // Make sure t_ctid is set correctly (self link; no partition-move here).
        set_tuple_ctid(bytes, tuple_off, blkno, xlrec.offnum);

        page.set_lsn(record.next_lsn);
        drop(guard);
        pool.mark_buffer_dirty(rb.buffer);
    }
    release_if_valid(shared, rb.buffer);
}

/// PG `heap_xlog_update`: stamp the old tuple's xmax + infomask (block 1), and
/// insert the new tuple (block 0) -- the new tuple may be on the same or a
/// different page.
async fn heap_xlog_update(shared: &Arc<SharedState>, record: &DecodedXLogRecord, _hot: bool) {
    let main = record.get_data().unwrap_or_panic_with(|| "heap update redo: missing main data");
    assert!(main.len() >= SizeOfHeapUpdate);
    let xlrec = xl_heap_update {
        old_xmax: TransactionId(u32::from_ne_bytes([main[0], main[1], main[2], main[3]])),
        old_offnum: u16::from_ne_bytes([main[4], main[5]]),
        old_infobits_set: main[6],
        flags: main[7],
        new_xmax: TransactionId(u32::from_ne_bytes([main[8], main[9], main[10], main[11]])),
        new_offnum: u16::from_ne_bytes([main[12], main[13]]),
    };

    // The new tuple is registered as block 0 (the running system's emit_update_wal
    // registers block 1 = new page, block 0 = old page). We follow the recorded
    // block tags: the OLD page is where old_offnum lives, the NEW page where the
    // new tuple is inserted. emit_update_wal uses block 1 for new, block 0 for old.
    // Apply the old-tuple update first, then the new-tuple insert.

    // --- old tuple: block 0 (old page) ---
    let rb_old = xlog_read_buffer_for_redo(shared, record, 0).await;
    if rb_old.action == XLogRedoAction::BLK_NEEDS_REDO {
        let pool = shared.buffers();
        let buf_id = rb_old.buffer.as_global().unwrap_or_panic_with(|| "redo: shared buffer expected") as i32;
        let guard = pool.content_exclusive(rb_old.buffer);
        // SAFETY: content-exclusive lock held; sole writer.
        let page = unsafe { pool.block_mut(buf_id) };
        let item_id = page.get_item_id(xlrec.old_offnum);
        let tuple_off = item_id_offset(item_id);
        let (_, _, newblk) = record.get_block_tag(1);
        let bytes = page.as_mut_bytes();
        // Stamp xmax + cmax + infobits (non-HOT update: ClearHotUpdated).
        stamp_xmax_infobits(bytes, tuple_off, xlrec.old_xmax, xlrec.old_infobits_set);
        // Set the forward chain link in t_ctid to the new version's TID.
        set_tuple_ctid(bytes, tuple_off, newblk, xlrec.new_offnum);
        page.set_lsn(record.next_lsn);
        drop(guard);
        pool.mark_buffer_dirty(rb_old.buffer);
    }
    release_if_valid(shared, rb_old.buffer);

    // --- new tuple: block 1 (new page) ---
    if xlrec.flags & XLH_UPDATE_CONTAINS_NEW_TUPLE != 0 {
        let (_, _, newblk) = record.get_block_tag(1);
        let init_page = record.info() & XLOG_HEAP_INIT_PAGE != 0;
        let rb_new = if init_page {
            xlog_init_buffer_for_redo(shared, record, 1).await
        } else {
            xlog_read_buffer_for_redo(shared, record, 1).await
        };
        if rb_new.action == XLogRedoAction::BLK_NEEDS_REDO {
            let pool = shared.buffers();
            let buf_id = rb_new.buffer.as_global().unwrap_or_panic_with(|| "redo: shared buffer expected") as i32;
            let block_data =
                record.get_block_data(1).unwrap_or_panic_with(|| "heap update redo: missing new block data").to_vec();
            assert!(block_data.len() > SizeOfHeapHeader);
            let xlhdr = read_xl_heap_header(&block_data);
            let body = &block_data[SizeOfHeapHeader..];
            let tuple = build_redo_tuple(&xlhdr, body, record.header.xid, newblk, xlrec.new_offnum);

            let guard = pool.content_exclusive(rb_new.buffer);
            // SAFETY: content-exclusive lock held; sole writer.
            let page = unsafe { pool.block_mut(buf_id) };
            if init_page {
                page.init(BLCKSZ as usize, 0);
            }
            let added = page.add_item_extended(
                &tuple,
                tuple.len(),
                xlrec.new_offnum,
                PageAddItemFlags::OVERWRITE | PageAddItemFlags::IS_HEAP,
            );
            assert_ne!(added, 0, "heap update redo: failed to add new tuple");
            page.set_lsn(record.next_lsn);
            drop(guard);
            pool.mark_buffer_dirty(rb_new.buffer);
        }
        release_if_valid(shared, rb_new.buffer);
    }
}

/// PG `heap_xlog_lock`: stamp a row-lock's xmax + lock bits on the tuple. Clears
/// HEAP_XMAX_BITS|HEAP_MOVED (infomask) and HEAP_KEYS_UPDATED (infomask2), applies
/// `infobits`, and -- when the resulting xmax is lock-only -- clears HOT-updated +
/// self-links t_ctid. Sets xmax + cmax = FirstCommandId (combo-cid cleared).
async fn heap_xlog_lock(shared: &Arc<SharedState>, record: &DecodedXLogRecord) {
    let main = record.get_data().unwrap_or_panic_with(|| "heap lock redo: missing main data");
    assert!(main.len() >= SizeOfHeapLock);
    let xlrec = xl_heap_lock {
        xmax: TransactionId(u32::from_ne_bytes([main[0], main[1], main[2], main[3]])),
        offnum: u16::from_ne_bytes([main[4], main[5]]),
        infobits_set: main[6],
        flags: main[7],
    };

    let rb = xlog_read_buffer_for_redo(shared, record, 0).await;
    if rb.action == XLogRedoAction::BLK_NEEDS_REDO {
        let pool = shared.buffers();
        let buf_id = rb.buffer.as_global().unwrap_or_panic_with(|| "redo: shared buffer expected") as i32;
        let (_, _, blkno) = record.get_block_tag(0);
        let guard = pool.content_exclusive(rb.buffer);
        // SAFETY: content-exclusive lock held; sole writer.
        let page = unsafe { pool.block_mut(buf_id) };
        let item_id = page.get_item_id(xlrec.offnum);
        let tuple_off = item_id_offset(item_id);
        let bytes = page.as_mut_bytes();

        let mut infomask = u16::from_ne_bytes([bytes[tuple_off + 20], bytes[tuple_off + 21]]);
        let mut infomask2 = u16::from_ne_bytes([bytes[tuple_off + 18], bytes[tuple_off + 19]]);
        infomask &= !(HEAP_XMAX_BITS | HEAP_MOVED);
        infomask2 &= !HEAP_KEYS_UPDATED;
        fix_infomask_from_infobits(xlrec.infobits_set, &mut infomask, &mut infomask2);
        // Lock-only xmax carries no update: clear HOT-updated + self-link t_ctid.
        if HEAP_XMAX_IS_LOCKED_ONLY(infomask) {
            infomask2 &= !HEAP_HOT_UPDATED;
            set_tuple_ctid(bytes, tuple_off, blkno, xlrec.offnum);
        }
        // cmax = FirstCommandId, iscombo = false -> clear HEAP_COMBOCID.
        infomask &= !HEAP_COMBOCID;
        bytes[tuple_off + 18..tuple_off + 20].copy_from_slice(&infomask2.to_ne_bytes());
        bytes[tuple_off + 20..tuple_off + 22].copy_from_slice(&infomask.to_ne_bytes());
        bytes[tuple_off + 4..tuple_off + 8].copy_from_slice(&xlrec.xmax.0.to_ne_bytes());
        bytes[tuple_off + 8..tuple_off + 12].copy_from_slice(&FIRST_COMMAND_ID.to_ne_bytes());

        page.set_lsn(record.next_lsn);
        drop(guard);
        pool.mark_buffer_dirty(rb.buffer);
    }
    release_if_valid(shared, rb.buffer);
}

/// PG `fix_infomask_from_infobits`: clear the xmax-lock bits from `infomask` and
/// HEAP_KEYS_UPDATED from `infomask2`, then set the bits the `infobits` byte
/// requests (the reverse of `compute_infobits`).
fn fix_infomask_from_infobits(infobits: u8, infomask: &mut u16, infomask2: &mut u16) {
    *infomask &=
        !(HEAP_XMAX_IS_MULTI | HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_KEYSHR_LOCK | HEAP_XMAX_EXCL_LOCK);
    *infomask2 &= !HEAP_KEYS_UPDATED;

    if infobits & XLHL_XMAX_IS_MULTI != 0 {
        *infomask |= HEAP_XMAX_IS_MULTI;
    }
    if infobits & XLHL_XMAX_LOCK_ONLY != 0 {
        *infomask |= HEAP_XMAX_LOCK_ONLY;
    }
    if infobits & XLHL_XMAX_EXCL_LOCK != 0 {
        *infomask |= HEAP_XMAX_EXCL_LOCK;
    }
    // note HEAP_XMAX_SHR_LOCK isn't considered here
    if infobits & XLHL_XMAX_KEYSHR_LOCK != 0 {
        *infomask |= HEAP_XMAX_KEYSHR_LOCK;
    }
    if infobits & XLHL_KEYS_UPDATED != 0 {
        *infomask2 |= HEAP_KEYS_UPDATED;
    }
}

/// The delete/non-HOT-update redo tuple stamping shared by both paths: clear
/// HEAP_XMAX_BITS + HEAP_MOVED (infomask) and HEAP_KEYS_UPDATED + HEAP_HOT_UPDATED
/// (infomask2), apply `infobits`, then set xmax and cmax = FirstCommandId with the
/// combo-cid flag cleared. Mirrors the common prologue of C `heap_xlog_delete` /
/// `heap_xlog_update` (non-HOT: ClearHotUpdated).
fn stamp_xmax_infobits(bytes: &mut [u8], tuple_off: usize, xmax: TransactionId, infobits: u8) {
    let mut infomask = u16::from_ne_bytes([bytes[tuple_off + 20], bytes[tuple_off + 21]]);
    let mut infomask2 = u16::from_ne_bytes([bytes[tuple_off + 18], bytes[tuple_off + 19]]);
    infomask &= !(HEAP_XMAX_BITS | HEAP_MOVED);
    infomask2 &= !(HEAP_KEYS_UPDATED | HEAP_HOT_UPDATED);
    fix_infomask_from_infobits(infobits, &mut infomask, &mut infomask2);
    // cmax = FirstCommandId, iscombo = false -> clear HEAP_COMBOCID.
    infomask &= !HEAP_COMBOCID;
    bytes[tuple_off + 18..tuple_off + 20].copy_from_slice(&infomask2.to_ne_bytes());
    bytes[tuple_off + 20..tuple_off + 22].copy_from_slice(&infomask.to_ne_bytes());
    // xmax = deleting/updating xid (offset 4..8), cmax = FirstCommandId (field3, 8..12).
    bytes[tuple_off + 4..tuple_off + 8].copy_from_slice(&xmax.0.to_ne_bytes());
    bytes[tuple_off + 8..tuple_off + 12].copy_from_slice(&FIRST_COMMAND_ID.to_ne_bytes());
}

/// Set a tuple header's t_ctid = (blk, off) (offsets 12..16 = block, 16..18 = off).
fn set_tuple_ctid(bytes: &mut [u8], tuple_off: usize, blk: u32, off: OffsetNumber) {
    bytes[tuple_off + 12..tuple_off + 16].copy_from_slice(&blk.to_ne_bytes());
    bytes[tuple_off + 16..tuple_off + 18].copy_from_slice(&off.to_ne_bytes());
}

/// The byte offset of a tuple on its page, from its ItemId (`lp_off`).
fn item_id_offset(item_id: crate::storage::itemid::ItemIdData) -> usize {
    item_id.lp_off() as usize
}

fn release_if_valid(shared: &Arc<SharedState>, buffer: crate::storage::buf::Buffer) {
    if buffer != crate::storage::buf::INVALID_BUFFER {
        shared.buffers().release_buffer(buffer);
    }
}
