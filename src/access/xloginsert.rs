//! Translated from PostgreSQL src/include/access/xloginsert.h
//!
//! STUB (foundation-rewrite: wal). Functions for generating WAL records. The
//! REGBUF_* flags are translated; insert API bodies are `// TODO(wal)`.

use bitflags::bitflags;

use crate::access::rmgr::RmgrId;
use crate::access::xlogdefs::XLogRecPtr;
use crate::common::relpath::ForkNumber;
use crate::storage::block::BlockNumber;
use crate::storage::buf::Buffer;
use crate::storage::bufpage::{Page, PageData};
use crate::storage::relfilelocator::RelFileLocator;
use crate::utils::relcache::Relation;

/// The minimum size of the WAL construction working area; call
/// `XLogEnsureRecordSpace` to grow beyond these.
pub const XLR_NORMAL_MAX_BLOCK_ID: i32 = 4;
pub const XLR_NORMAL_RDATAS: i32 = 20;

bitflags! {
    /// Flags for XLogRegisterBuffer (PARTIAL: composite `WILL_INIT = 0x06`).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct RegBuf: u8 {
        const FORCE_IMAGE = 0x01;          // force a full-page image
        const NO_IMAGE    = 0x02;          // don't take a full-page image
        // page will be re-initialized at replay (implies NO_IMAGE)
        const WILL_INIT   = 0x04 | 0x02;
        const STANDARD    = 0x08;          // page follows "standard" layout
        const KEEP_DATA   = 0x10;          // include data even with a full-page image
        const NO_CHANGE   = 0x20;          // intentionally register clean buffer
    }
}

// prototypes for public functions in xloginsert.c:
pub fn XLogBeginInsert() {
    unimplemented!() // TODO(wal)
}
pub fn XLogSetRecordFlags(_flags: u8) {
    unimplemented!() // TODO(wal)
}
pub fn XLogInsert(_rmid: RmgrId, _info: u8) -> XLogRecPtr {
    unimplemented!() // TODO(wal)
}
pub fn XLogEnsureRecordSpace(_max_block_id: i32, _ndatas: i32) {
    unimplemented!() // TODO(wal)
}
pub fn XLogRegisterData(_data: &[u8]) {
    unimplemented!() // TODO(wal)
}
pub fn XLogRegisterBuffer(_block_id: u8, _buffer: Buffer, _flags: RegBuf) {
    unimplemented!() // TODO(wal)
}
pub fn XLogRegisterBlock(
    _block_id: u8,
    _rlocator: &RelFileLocator,
    _forknum: ForkNumber,
    _blknum: BlockNumber,
    _page: &PageData,
    _flags: RegBuf,
) {
    unimplemented!() // TODO(wal)
}
pub fn XLogRegisterBufData(_block_id: u8, _data: &[u8]) {
    unimplemented!() // TODO(wal)
}
pub fn XLogResetInsertion() {
    unimplemented!() // TODO(wal)
}
pub fn XLogCheckBufferNeedsBackup(_buffer: Buffer) -> bool {
    unimplemented!() // TODO(wal)
}

pub fn log_newpage(
    _rlocator: &RelFileLocator,
    _forknum: ForkNumber,
    _blkno: BlockNumber,
    _page: Page,
    _page_std: bool,
) -> XLogRecPtr {
    unimplemented!() // TODO(wal)
}
pub fn log_newpages(
    _rlocator: &RelFileLocator,
    _forknum: ForkNumber,
    _blknos: &[BlockNumber],
    _pages: &[Page],
    _page_std: bool,
) {
    unimplemented!() // TODO(wal)
}
pub fn log_newpage_buffer(_buffer: Buffer, _page_std: bool) -> XLogRecPtr {
    unimplemented!() // TODO(wal)
}
pub fn log_newpage_range(
    _rel: Relation,
    _forknum: ForkNumber,
    _startblk: BlockNumber,
    _endblk: BlockNumber,
    _page_std: bool,
) {
    unimplemented!() // TODO(wal)
}
pub fn XLogSaveBufferForHint(_buffer: Buffer, _buffer_std: bool) -> XLogRecPtr {
    unimplemented!() // TODO(wal)
}

pub fn InitXLogInsert() {
    unimplemented!() // TODO(wal)
}
