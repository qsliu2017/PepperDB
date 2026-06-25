//! Translated from PostgreSQL src/backend/access/transam/xloginsert.c
//!
//! WAL record *assembly*: stage the main data, block references and full-page
//! images for a record (via the `XLogBeginInsert` / `XLogRegister*` calls), then
//! pack them into the on-disk record byte layout and hand the bytes to Part A's
//! [`XLogCtl::insert_record`] (PG `XLogInsertRecord`).
//!
//! On-disk fidelity: [`assemble`] reproduces xloginsert.c `XLogRecordAssemble`
//! byte-for-byte -- the fixed `XLogRecord` header, then per-block
//! `XLogRecordBlockHeader` (+ optional image/compress headers + RelFileLocator +
//! BlockNumber), then the origin / top-xid / main-data id bytes, then the data
//! and page images -- so xlogreader (13C) and real PostgreSQL can decode it. The
//! record CRC is CRC-32C over (block/data area .. then the header), matching the
//! C ordering. We do not compress page images (wal_compression = none here), so
//! the compress-header path is omitted as it is in a no-compression build.
//!
//! Per-task staging (Send): the registered buffers / data chain are kept in a
//! per-task [`Insertion`] published as a tokio `task_local` `RefCell`. It holds
//! only owned/`Copy` data (no `Rc`/raw pointers), and the `RefCell` borrow is
//! never held across an `.await`: [`XLogInsert`] snapshots the fully-assembled
//! record into an owned `Vec<u8>` while holding the borrow, drops it, and only
//! then awaits [`XLogCtl::insert_record`]. So the future is `Send` and the
//! single await in the begin->register->insert sequence holds nothing borrowed.

use std::cell::RefCell;
use std::sync::Arc;

use crate::access::rmgr::RmgrId;
use crate::access::xlog::XLogRecordFlags;
use crate::access::xloginsert::RegBuf;
use crate::access::xlogrecord::{
    SizeOfXLogRecord, SizeOfXLogRecordBlockHeader, SizeOfXLogRecordBlockImageHeader,
    XLR_BLOCK_ID_DATA_LONG, XLR_BLOCK_ID_DATA_SHORT, XLR_MAX_BLOCK_ID, XLogRecordMaxSize,
    BKPBLOCK_HAS_DATA, BKPBLOCK_HAS_IMAGE, BKPBLOCK_SAME_REL, BKPBLOCK_WILL_INIT,
};
use crate::access::xlogrecord::BkpImage;
use crate::backend::access::transam::xlog::XLogCtl;
use crate::catalog::pg_control::XLOG_FPI;
use crate::common::relpath::ForkNumber;
use crate::pg_config::XLOG_BLCKSZ;
use crate::port::pg_crc32c::{comp_crc32c, init_crc32c, pg_crc32c};
use crate::storage::block::BlockNumber;
use crate::storage::bufpage::Page;
use crate::storage::relfilelocator::RelFileLocator;
use crate::access::xlogdefs::{XLogRecPtr, INVALID_XLOG_REC_PTR};
// SizeOfPageHeaderData bounds the standard-page "hole" elimination check.
use crate::storage::bufpage::SizeOfPageHeaderData;

const BLCKSZ: usize = XLOG_BLCKSZ as usize;

/// One registered block reference (PG `registered_buffer`, the staging half).
#[derive(Clone)]
struct RegisteredBlock {
    in_use: bool,
    flags: RegBuf,
    rlocator: RelFileLocator,
    forkno: ForkNumber,
    block: BlockNumber,
    /// Page image bytes (the whole BLCKSZ page); `None` when no buffer page was
    /// registered (should not happen for in-use blocks here).
    page: Option<Box<[u8]>>,
    /// Page LSN, read from the registered page (for the FPW decision).
    page_lsn: XLogRecPtr,
    /// Buffer-specific data appended via [`XLogRegisterBufData`].
    rdata: Vec<u8>,
}

impl RegisteredBlock {
    fn empty() -> RegisteredBlock {
        RegisteredBlock {
            in_use: false,
            flags: RegBuf::empty(),
            rlocator: RelFileLocator {
                spcOid: crate::postgres_ext::Oid(0),
                dbOid: crate::postgres_ext::Oid(0),
                relNumber: crate::postgres_ext::Oid(0),
            },
            forkno: ForkNumber::MAIN_FORKNUM,
            block: 0,
            page: None,
            page_lsn: INVALID_XLOG_REC_PTR,
            rdata: Vec::new(),
        }
    }
}

/// Per-task WAL-record construction state (PG's `registered_buffers` /
/// `mainrdata*` / `curinsert_flags` statics, made per-task).
struct Insertion {
    begininsert_called: bool,
    max_registered_block_id: usize,
    blocks: Vec<RegisteredBlock>,
    mainrdata: Vec<u8>,
    curinsert_flags: XLogRecordFlags,
}

impl Insertion {
    fn new() -> Insertion {
        Insertion {
            begininsert_called: false,
            max_registered_block_id: 0,
            blocks: (0..=XLR_MAX_BLOCK_ID as usize).map(|_| RegisteredBlock::empty()).collect(),
            mainrdata: Vec::new(),
            curinsert_flags: XLogRecordFlags::empty(),
        }
    }
    fn reset(&mut self) {
        for i in 0..self.max_registered_block_id {
            self.blocks[i].in_use = false;
            self.blocks[i].rdata.clear();
            self.blocks[i].page = None;
        }
        self.max_registered_block_id = 0;
        self.mainrdata.clear();
        self.curinsert_flags = XLogRecordFlags::empty();
        self.begininsert_called = false;
    }
}

tokio::task_local! {
    /// The current task's WAL-record construction state. Established by
    /// [`with_insertion`]; the `RefCell` is borrowed only synchronously (never
    /// across an `.await`), so the construction sequence stays `Send`.
    static INSERTION: RefCell<Insertion>;
}

/// Run `fut` with a fresh per-task [`Insertion`] in scope. A backend task wraps
/// its body in this once; the self-contained `log_newpage*` helpers and tests
/// scope each call. Mirrors PG allocating the per-backend construction area
/// (`InitXLogInsert`) once per process.
pub async fn with_insertion<F, T>(fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    INSERTION.scope(RefCell::new(Insertion::new()), fut).await
}

fn with_state<R>(f: impl FnOnce(&mut Insertion) -> R) -> R {
    INSERTION.with(|cell| f(&mut cell.borrow_mut()))
}

/// PG `XLogBeginInsert`: begin constructing a WAL record.
pub fn begin_insert() {
    with_state(|s| {
        assert_eq!(s.max_registered_block_id, 0);
        assert!(s.mainrdata.is_empty());
        assert!(!s.begininsert_called, "XLogBeginInsert was already called");
        s.begininsert_called = true;
    });
}

/// PG `XLogSetRecordFlags`: set status flags for the in-progress record.
pub fn set_record_flags(flags: XLogRecordFlags) {
    with_state(|s| {
        assert!(s.begininsert_called);
        s.curinsert_flags |= flags;
    });
}

/// PG `XLogResetInsertion`: discard the in-progress record construction state.
pub fn reset_insertion() {
    with_state(|s| s.reset());
}

/// PG `XLogRegisterData`: append `data` to the record's main data chunk.
pub fn register_data(data: &[u8]) {
    with_state(|s| {
        assert!(s.begininsert_called);
        s.mainrdata.extend_from_slice(data);
    });
}

/// PG `XLogRegisterBlock`: register a block not backed by a shared buffer (the
/// caller supplies the page image and its identity directly).
pub fn register_block(
    block_id: u8,
    rlocator: &RelFileLocator,
    forknum: ForkNumber,
    blknum: BlockNumber,
    page: &Page,
    flags: RegBuf,
) {
    with_state(|s| {
        assert!(s.begininsert_called);
        let id = block_id as usize;
        assert!(id <= XLR_MAX_BLOCK_ID as usize, "too many registered buffers");
        if id >= s.max_registered_block_id {
            s.max_registered_block_id = id + 1;
        }
        let lsn = page_lsn(page);
        let regbuf = &mut s.blocks[id];
        regbuf.rlocator = *rlocator;
        regbuf.forkno = forknum;
        regbuf.block = blknum;
        regbuf.page = Some(page.as_bytes().to_vec().into_boxed_slice());
        regbuf.page_lsn = lsn;
        regbuf.flags = flags;
        regbuf.rdata.clear();
        regbuf.in_use = true;
    });
}

/// PG `XLogRegisterBufData`: append buffer-specific data for a previously
/// registered block.
pub fn register_buf_data(block_id: u8, data: &[u8]) {
    with_state(|s| {
        assert!(s.begininsert_called);
        let regbuf = &mut s.blocks[block_id as usize];
        assert!(regbuf.in_use, "no block with id {block_id} registered with WAL insertion");
        assert!(
            regbuf.rdata.len() + data.len() <= u16::MAX as usize,
            "too much per-block WAL data"
        );
        regbuf.rdata.extend_from_slice(data);
    });
}

/// The page LSN is the first 8 bytes of every page (`PageGetLSN`), regardless of
/// whether the page uses the standard layout.
fn page_lsn(page: &Page) -> XLogRecPtr {
    let b = page.as_bytes();
    XLogRecPtr(u64::from_ne_bytes(b[0..8].try_into().unwrap()))
}

/// (rec_len, fpi_len) helpers: the lower/upper of a standard page (for hole
/// elimination), read straight from the page header bytes.
fn standard_hole(page: &[u8]) -> Option<(u16, u16)> {
    let lower = u16::from_ne_bytes([page[12], page[13]]);
    let upper = u16::from_ne_bytes([page[14], page[15]]);
    if lower as usize >= SizeOfPageHeaderData && upper > lower && upper as usize <= BLCKSZ {
        Some((lower, upper))
    } else {
        None
    }
}

/// The result of [`assemble`]: the on-disk record bytes plus the FPW recheck
/// info that [`XLogInsert`] needs.
struct Assembled {
    bytes: Vec<u8>,
    /// Partial (non-finalized) CRC-32C accumulated over the record BODY only
    /// (everything after the 24-byte `XLogRecord` header). The insert path folds
    /// in the header (with the real `xl_prev`) and finalizes it -- mirrors C's
    /// `rdata_crc` carried from `XLogRecordAssemble` to `XLogInsertRecord`.
    partial_crc: pg_crc32c,
    /// Lowest page LSN among blocks that did NOT get a full-page image; if the
    /// redo pointer advances past this between assembly and insert, the record
    /// must be reassembled with images. `Invalid` if all blocks were imaged.
    fpw_lsn: XLogRecPtr,
}

/// PG `XLogRecordAssemble`: build the on-disk record from the staged data.
///
/// `redo_rec_ptr` / `do_page_writes` are the full-page-write inputs sampled by
/// the caller; a block gets a full-page image when forced, or when page writes
/// are on and the page has not been written since the redo point.
fn assemble(
    s: &Insertion,
    rmid: RmgrId,
    info: u8,
    redo_rec_ptr: XLogRecPtr,
    do_page_writes: bool,
) -> Assembled {
    // Area 1: the per-block headers + origin/topxid/main-data id bytes (the part
    // the C calls `hdr_scratch`, everything except the fixed record header).
    let mut hdr: Vec<u8> = Vec::new();
    // Area 2: the data/image payload, in the same order the headers reference.
    let mut payload: Vec<u8> = Vec::new();

    let mut total_len: u64 = 0;
    let mut fpw_lsn = INVALID_XLOG_REC_PTR;
    let mut prev_rlocator: Option<RelFileLocator> = None;

    for block_id in 0..s.max_registered_block_id {
        let regbuf = &s.blocks[block_id];
        if !regbuf.in_use {
            continue;
        }
        let page = regbuf.page.as_deref().expect("registered block without page");

        // Decide whether this block needs a full-page image.
        let needs_backup = if regbuf.flags.contains(RegBuf::FORCE_IMAGE) {
            true
        } else if regbuf.flags.contains(RegBuf::NO_IMAGE) {
            false
        } else if !do_page_writes {
            false
        } else {
            let needs = regbuf.page_lsn <= redo_rec_ptr;
            if !needs && (fpw_lsn.is_invalid() || regbuf.page_lsn < fpw_lsn) {
                fpw_lsn = regbuf.page_lsn;
            }
            needs
        };

        // Decide whether buffer data is included.
        let needs_data = if regbuf.rdata.is_empty() {
            false
        } else if regbuf.flags.contains(RegBuf::KEEP_DATA) {
            true
        } else {
            !needs_backup
        };

        // XLogRecordBlockHeader: id, fork_flags, data_length.
        let mut fork_flags = regbuf.forkno as u8;
        if regbuf.flags.contains(RegBuf::WILL_INIT) {
            fork_flags |= BKPBLOCK_WILL_INIT;
        }

        // We never enable wal_consistency_checking here, so an image is included
        // exactly when the block needs to be backed up.
        let include_image = needs_backup;

        // Build the block-image header bytes if including an image.
        let mut bimg_bytes: Vec<u8> = Vec::new();
        if include_image {
            fork_flags |= BKPBLOCK_HAS_IMAGE;
            let (hole_offset, hole_length) = if regbuf.flags.contains(RegBuf::STANDARD) {
                match standard_hole(page) {
                    Some((lower, upper)) => (lower, upper - lower),
                    None => (0, 0),
                }
            } else {
                (0, 0)
            };

            let length: u16 = (BLCKSZ as u16) - hole_length;
            let mut bimg_info = if hole_length == 0 { BkpImage::empty() } else { BkpImage::HAS_HOLE };
            // needs_backup is always true here (no consistency checking path).
            bimg_info |= BkpImage::APPLY;

            // XLogRecordBlockImageHeader: length(u16), hole_offset(u16), bimg_info(u8).
            bimg_bytes.extend_from_slice(&length.to_ne_bytes());
            bimg_bytes.extend_from_slice(&hole_offset.to_ne_bytes());
            bimg_bytes.push(bimg_info.bits());
            // No compress header: wal_compression = none.

            // Page image payload, skipping the hole.
            if hole_length == 0 {
                payload.extend_from_slice(&page[..BLCKSZ]);
            } else {
                payload.extend_from_slice(&page[..hole_offset as usize]);
                let after = hole_offset as usize + hole_length as usize;
                payload.extend_from_slice(&page[after..BLCKSZ]);
            }
            total_len += length as u64;
        }

        let mut data_length: u16 = 0;
        if needs_data {
            fork_flags |= BKPBLOCK_HAS_DATA;
            data_length = regbuf.rdata.len() as u16;
            total_len += regbuf.rdata.len() as u64;
        }

        let samerel = matches!(prev_rlocator, Some(prev) if prev.equals(&regbuf.rlocator));
        let final_fork_flags = if samerel { fork_flags | BKPBLOCK_SAME_REL } else { fork_flags };
        prev_rlocator = Some(regbuf.rlocator);

        // Emit the block header (id, fork_flags, data_length).
        hdr.push(block_id as u8);
        hdr.push(final_fork_flags);
        hdr.extend_from_slice(&data_length.to_ne_bytes());
        debug_assert_eq!(
            hdr.len() % 1,
            0,
            "block header is {} bytes",
            SizeOfXLogRecordBlockHeader
        );
        if include_image {
            debug_assert_eq!(bimg_bytes.len(), SizeOfXLogRecordBlockImageHeader);
            hdr.extend_from_slice(&bimg_bytes);
        }
        if !samerel {
            // RelFileLocator: spcOid, dbOid, relNumber (3 x u32), on-disk order.
            hdr.extend_from_slice(&regbuf.rlocator.spcOid.0.to_ne_bytes());
            hdr.extend_from_slice(&regbuf.rlocator.dbOid.0.to_ne_bytes());
            hdr.extend_from_slice(&regbuf.rlocator.relNumber.0.to_ne_bytes());
        }
        hdr.extend_from_slice(&regbuf.block.to_ne_bytes());

        // Now append this block's buffer data to the payload (after its header,
        // matching the rdata chain order: all headers reference payload that
        // follows in registration order).
        if needs_data {
            payload.extend_from_slice(&regbuf.rdata);
        }
    }

    // Origin / top-xid: not produced in the foundation (no replication origin
    // session, no subxact top-xid logging yet), so those id bytes are omitted,
    // exactly as PG omits them when the conditions are false.

    // Main data id byte + length, then the data joins the payload.
    if !s.mainrdata.is_empty() {
        let len = s.mainrdata.len();
        if len > 255 {
            assert!(len <= u32::MAX as usize, "too much WAL main data");
            hdr.push(XLR_BLOCK_ID_DATA_LONG);
            hdr.extend_from_slice(&(len as u32).to_ne_bytes());
        } else {
            hdr.push(XLR_BLOCK_ID_DATA_SHORT);
            hdr.push(len as u8);
        }
        payload.extend_from_slice(&s.mainrdata);
        total_len += len as u64;
    }

    total_len += (SizeOfXLogRecord + hdr.len()) as u64;
    assert!(total_len <= XLogRecordMaxSize as u64, "oversized WAL record");

    // Partial CRC: over the record BODY only (block/data header area, then the
    // payload), in C's order (rdata, backup blocks). NOT finalized and the fixed
    // record header is NOT folded -- the insert path fills the real `xl_prev`,
    // folds the header up to `xl_crc`, then finalizes. Mirrors C's `rdata_crc`.
    let mut partial_crc = init_crc32c();
    partial_crc = comp_crc32c(partial_crc, &hdr);
    partial_crc = comp_crc32c(partial_crc, &payload);

    // Assemble the final contiguous record: fixed header, then hdr, then payload.
    let mut bytes = Vec::with_capacity(total_len as usize);
    // XLogRecord header: tot_len(u32), xid(u32), prev(u64), info(u8), rmid(u8),
    // 2 pad, crc(u32). xl_prev is a 0 placeholder filled by the insert path from
    // the reservation; xl_crc is left 0 and written by the insert path after the
    // header is folded in. xl_xid is 0 here (no transaction context yet).
    bytes.extend_from_slice(&(total_len as u32).to_ne_bytes());
    bytes.extend_from_slice(&0u32.to_ne_bytes()); // xl_xid
    bytes.extend_from_slice(&INVALID_XLOG_REC_PTR.0.to_ne_bytes()); // xl_prev placeholder
    bytes.push(info);
    bytes.push(rmid);
    bytes.extend_from_slice(&[0u8, 0u8]); // padding
    bytes.extend_from_slice(&0u32.to_ne_bytes()); // xl_crc placeholder

    bytes.extend_from_slice(&hdr);
    bytes.extend_from_slice(&payload);
    debug_assert_eq!(bytes.len() as u64, total_len);

    Assembled { bytes, partial_crc, fpw_lsn }
}

/// PG `XLogInsert`: assemble the staged record and insert it into the WAL,
/// returning the end LSN. Handles the full-page-write recheck loop: if the redo
/// pointer advanced past `fpw_lsn` between assembly and the (sampled) decision,
/// reassemble with the new redo pointer so the now-stale page gets an image.
pub async fn xlog_insert(xlog: &Arc<XLogCtl>, rmid: RmgrId, info: u8) -> XLogRecPtr {
    // Validate the info mask (low bits except SPECIAL_REL_UPDATE/CHECK_CONSISTENCY
    // are reserved); matches the C PANIC on a bad mask.
    const RESERVED: u8 = !(crate::access::xlogrecord::XLR_RMGR_INFO_MASK
        | crate::access::xlogrecord::XLR_SPECIAL_REL_UPDATE
        | crate::access::xlogrecord::XLR_CHECK_CONSISTENCY);
    assert_eq!(info & RESERVED, 0, "invalid xlog info mask {info:02X}");

    assert!(
        with_state(|s| s.begininsert_called),
        "XLogBeginInsert was not called"
    );

    let end = loop {
        // Sample the full-page-write inputs. The redo pointer only advances at a
        // checkpoint; we have no checkpointer in the foundation, so it stays
        // Invalid and page writes are on (PG default). The recheck loop is still
        // implemented faithfully for when the checkpointer arrives.
        let redo_rec_ptr = xlog.get_redo_rec_ptr();
        let do_page_writes = true;

        // Assemble + snapshot the bytes while holding the borrow; release before
        // awaiting so nothing borrowed crosses the `.await`.
        let assembled =
            with_state(|s| assemble(s, rmid, info, redo_rec_ptr, do_page_writes));

        // FPW recheck: if a checkpoint advanced the redo pointer past the lowest
        // un-imaged page LSN, our decision is stale -> reassemble.
        let redo_now = xlog.get_redo_rec_ptr();
        if !assembled.fpw_lsn.is_invalid() && redo_now > redo_rec_ptr && assembled.fpw_lsn <= redo_now
        {
            continue;
        }

        break xlog.insert_record(&assembled.bytes, assembled.partial_crc).await;
    };

    reset_insertion();
    end
}

/// PG `XLogCheckBufferNeedsBackup` (page-image variant): does `page` need a
/// full-page image given the current redo pointer? (`buffer_std` is irrelevant
/// to the decision.) Used by callers deciding whether to take an FPI.
pub fn check_page_needs_backup(xlog: &Arc<XLogCtl>, page: &Page) -> bool {
    let redo = xlog.get_redo_rec_ptr();
    // do_page_writes is on (no checkpointer to turn it off).
    page_lsn(page) <= redo
}

// --- log_newpage family (FPI records) ------------------------------------

/// PG `log_newpage`: write an XLOG_FPI record carrying a full image of `page`,
/// then stamp the page's LSN (unless it is new). Returns the record end LSN.
pub async fn log_newpage(
    xlog: &Arc<XLogCtl>,
    rlocator: &RelFileLocator,
    forknum: ForkNumber,
    blkno: BlockNumber,
    page: &Page,
    page_std: bool,
) -> XLogRecPtr {
    let mut flags = RegBuf::FORCE_IMAGE;
    if page_std {
        flags |= RegBuf::STANDARD;
    }
    with_insertion(async move {
        begin_insert();
        register_block(0, rlocator, forknum, blkno, page, flags);
        xlog_insert(xlog, crate::access::rmgrlist::RmgrId::Xlog as RmgrId, XLOG_FPI).await
    })
    .await
}

/// PG `log_newpages`: like [`log_newpage`] but batches up to `XLR_MAX_BLOCK_ID`
/// pages per record. Stamps each non-new page's LSN is the caller's job here
/// (the bulk writer owns the pages); we just emit the records.
pub async fn log_newpages(
    xlog: &Arc<XLogCtl>,
    rlocator: &RelFileLocator,
    forknum: ForkNumber,
    blknos: &[BlockNumber],
    pages: &[&Page],
    page_std: bool,
) {
    assert_eq!(blknos.len(), pages.len());
    let mut flags = RegBuf::FORCE_IMAGE;
    if page_std {
        flags |= RegBuf::STANDARD;
    }
    let batch = XLR_MAX_BLOCK_ID as usize;
    let mut i = 0;
    while i < pages.len() {
        with_insertion(async {
            begin_insert();
            let mut nbatch = 0;
            while nbatch < batch && i < pages.len() {
                register_block(nbatch as u8, rlocator, forknum, blknos[i], pages[i], flags);
                i += 1;
                nbatch += 1;
            }
            xlog_insert(xlog, crate::access::rmgrlist::RmgrId::Xlog as RmgrId, XLOG_FPI)
                .await;
        })
        .await;
    }
}

/// PG `log_newpage_range` (FPI of blocks `startblk..endblk` of one fork). The
/// buffer-pool reads are deferred (no buffer manager wiring here); the caller
/// supplies the page images via [`log_newpages`]. This thin wrapper logs a
/// contiguous already-materialized slice of pages.
pub async fn log_newpage_range(
    xlog: &Arc<XLogCtl>,
    rlocator: &RelFileLocator,
    forknum: ForkNumber,
    startblk: BlockNumber,
    pages: &[&Page],
    page_std: bool,
) {
    let blknos: Vec<BlockNumber> = (0..pages.len() as u32).map(|i| startblk + i).collect();
    log_newpages(xlog, rlocator, forknum, &blknos, pages, page_std).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::rmgrlist::RmgrId as BuiltinRmgrId;
    use crate::access::xlog_internal::XLOGDIR;
    use crate::access::xlogrecord::SizeOfXLogRecord;
    use crate::backend::utils::init::globals::ProcessConfig;
    use crate::postgres_ext::Oid;
    use crate::storage::io_backend::IoBackend;

    async fn fresh_xlog() -> (Arc<XLogCtl>, std::path::PathBuf) {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let uniq = SEQ.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "pepperdb_xloginsert_{}_{}",
            std::process::id(),
            uniq
        ));
        crate::storage::io_backend::mkdir_all(dir.join(XLOGDIR)).await.unwrap();
        let io = Arc::new(IoBackend::with_default_budget());
        let config = Arc::new(ProcessConfig::new());
        config.set_data_dir(dir.to_str().unwrap());
        (XLogCtl::new(io, config), dir)
    }

    fn xlog_rmid() -> RmgrId {
        BuiltinRmgrId::Xlog as u8
    }

    #[tokio::test]
    async fn assemble_insert_and_flush() {
        let (xlog, dir) = fresh_xlog().await;
        let payload = b"hello WAL record";

        let end = with_insertion(async {
            begin_insert();
            register_data(payload);
            xlog_insert(&xlog, xlog_rmid(), 0x00).await
        })
        .await;

        assert!(end.is_valid());
        assert!(xlog.get_flush_rec_ptr().0 < end.0);
        xlog.xlog_flush(end).await;
        assert!(xlog.get_flush_rec_ptr().0 >= end.0);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn assembled_header_and_partial_crc_are_correct() {
        use crate::port::pg_crc32c::fin_crc32c;
        let (_xlog, dir) = fresh_xlog().await;
        let payload = b"abcdefghij"; // 10 bytes main data -> short data header

        // Assemble directly (no insert) so we can inspect the bytes + partial CRC.
        let asm = with_insertion(async {
            begin_insert();
            register_data(payload);
            with_state(|s| assemble(s, xlog_rmid(), 0x00, INVALID_XLOG_REC_PTR, true))
        })
        .await;
        let bytes = asm.bytes;

        // Header fields.
        let tot_len = u32::from_ne_bytes(bytes[0..4].try_into().unwrap());
        assert_eq!(tot_len as usize, bytes.len());
        let info = bytes[16];
        let rmid = bytes[17];
        assert_eq!(info, 0x00);
        assert_eq!(rmid, xlog_rmid());
        // assemble leaves xl_prev and xl_crc as zero placeholders (the insert path
        // fills them); it does NOT finalize the CRC.
        assert_eq!(&bytes[8..16], &0u64.to_ne_bytes(), "xl_prev placeholder");
        assert_eq!(&bytes[20..24], &0u32.to_ne_bytes(), "xl_crc placeholder");

        // partial_crc is the body-only running CRC; the final CRC equals
        // fin(comp(partial, header-up-to-xl_crc)). With xl_prev still 0 here, this
        // matches recomputing the whole thing the PG way (data area, then header).
        let final_crc = fin_crc32c(comp_crc32c(asm.partial_crc, &bytes[..SizeOfXLogRecord - 4]));
        let mut whole = init_crc32c();
        whole = comp_crc32c(whole, &bytes[SizeOfXLogRecord..]);
        whole = comp_crc32c(whole, &bytes[..SizeOfXLogRecord - 4]);
        whole = fin_crc32c(whole);
        assert_eq!(final_crc, whole, "partial CRC + header fold must match one-shot");

        // The main-data id byte (short) + length must be present and the payload
        // must follow at the end.
        let data_id = bytes[SizeOfXLogRecord];
        assert_eq!(data_id, XLR_BLOCK_ID_DATA_SHORT);
        assert_eq!(bytes[SizeOfXLogRecord + 1], payload.len() as u8);
        assert_eq!(&bytes[bytes.len() - payload.len()..], payload);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn log_newpage_emits_fpi() {
        let (xlog, dir) = fresh_xlog().await;
        let rloc = RelFileLocator { spcOid: Oid(1663), dbOid: Oid(5), relNumber: Oid(16384) };
        let mut page = Page::zeroed();
        // Make it a standard page with a hole: set pd_lower=40, pd_upper=8000.
        page.as_mut_bytes()[12..14].copy_from_slice(&40u16.to_ne_bytes());
        page.as_mut_bytes()[14..16].copy_from_slice(&8000u16.to_ne_bytes());

        let end = log_newpage(&xlog, &rloc, ForkNumber::MAIN_FORKNUM, 7, &page, true).await;
        assert!(end.is_valid());
        xlog.xlog_flush(end).await;
        assert!(xlog.get_flush_rec_ptr().0 >= end.0);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn log_newpages_batches() {
        let (xlog, dir) = fresh_xlog().await;
        let rloc = RelFileLocator { spcOid: Oid(1663), dbOid: Oid(5), relNumber: Oid(16385) };
        // More pages than one batch (XLR_MAX_BLOCK_ID) to force multiple records.
        let n = XLR_MAX_BLOCK_ID as usize + 5;
        let owned: Vec<Page> = (0..n).map(|_| Page::zeroed()).collect();
        let pages: Vec<&Page> = owned.iter().collect();
        let blknos: Vec<BlockNumber> = (0..n as u32).collect();

        log_newpages(&xlog, &rloc, ForkNumber::MAIN_FORKNUM, &blknos, &pages, false).await;
        let end = xlog.get_xlog_insert_rec_ptr();
        xlog.xlog_flush(end).await;
        assert!(xlog.get_flush_rec_ptr().0 >= end.0);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
