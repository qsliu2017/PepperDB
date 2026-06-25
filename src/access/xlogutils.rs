//! Translated from PostgreSQL src/include/access/xlogutils.h
//!
//! Utilities for replaying WAL records.

use crate::access::xlogdefs::{TimeLineID, XLogRecPtr, XLogSegNo};
use crate::access::xlogreader::{WALReadError, XLogReaderState};
use crate::common::relpath::ForkNumber;
use crate::storage::block::BlockNumber;
use crate::storage::buf::Buffer;
use crate::storage::bufmgr::ReadBufferMode;
use crate::storage::relfilelocator::RelFileLocator;
use crate::postgres_ext::Oid;
use crate::utils::relcache::Relation;

/// GUC variable.
pub static mut ignore_invalid_pages: bool = false;

/// Valid only in the startup process; indicates recovery actions.
pub static mut InRecovery: bool = false;

/// Hot-standby state, valid only in the startup process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum HotStandbyState {
    DISABLED,
    INITIALIZED,
    SNAPSHOT_PENDING,
    SNAPSHOT_READY,
}

pub static mut standbyState: HotStandbyState = HotStandbyState::DISABLED;

pub fn InHotStandby() -> bool {
    unsafe { standbyState >= HotStandbyState::SNAPSHOT_PENDING }
}

pub fn XLogHaveInvalidPages() -> bool {
    unimplemented!()
}

pub fn XLogCheckInvalidPages() {
    unimplemented!()
}

pub fn XLogDropRelation(rlocator: RelFileLocator, forknum: ForkNumber) {
    let _ = (rlocator, forknum);
    unimplemented!()
}

pub fn XLogDropDatabase(dbid: Oid) {
    let _ = dbid;
    unimplemented!()
}

pub fn XLogTruncateRelation(rlocator: RelFileLocator, fork_num: ForkNumber, nblocks: BlockNumber) {
    let _ = (rlocator, fork_num, nblocks);
    unimplemented!()
}

/// Result codes for XLogReadBufferForRedo[Extended].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XLogRedoAction {
    NEEDS_REDO, // changes from WAL record need to be applied
    DONE,       // block is already up-to-date
    RESTORED,   // block was restored from a full-page image
    NOTFOUND,   // block was not found (and hence need not be replayed)
}

/// Private data of the read_local_xlog_page_no_wait callback.
pub struct ReadLocalXLogPageNoWaitPrivate {
    pub end_of_wal: bool, // true when end of WAL is reached
}

/// C `Buffer *buf` out-param folds into the returned tuple.
pub fn XLogReadBufferForRedo(
    record: &mut XLogReaderState,
    block_id: u8,
) -> (XLogRedoAction, Buffer) {
    let _ = (record, block_id);
    unimplemented!()
}

pub fn XLogInitBufferForRedo(record: &mut XLogReaderState, block_id: u8) -> Buffer {
    let _ = (record, block_id);
    unimplemented!()
}

/// C `Buffer *buf` out-param folds into the returned tuple.
pub fn XLogReadBufferForRedoExtended(
    record: &mut XLogReaderState,
    block_id: u8,
    mode: ReadBufferMode,
    get_cleanup_lock: bool,
) -> (XLogRedoAction, Buffer) {
    let _ = (record, block_id, mode, get_cleanup_lock);
    unimplemented!()
}

pub fn XLogReadBufferExtended(
    rlocator: RelFileLocator,
    forknum: ForkNumber,
    blkno: BlockNumber,
    mode: ReadBufferMode,
    recent_buffer: Buffer,
) -> Buffer {
    let _ = (rlocator, forknum, blkno, mode, recent_buffer);
    unimplemented!()
}

pub fn CreateFakeRelcacheEntry(rlocator: RelFileLocator) -> Relation {
    let _ = rlocator;
    unimplemented!()
}

pub fn FreeFakeRelcacheEntry(fakerel: Relation) {
    let _ = fakerel;
    unimplemented!()
}

/// Returns the number of bytes read (C `int`).
pub fn read_local_xlog_page(
    state: &mut XLogReaderState,
    target_page_ptr: XLogRecPtr,
    req_len: i32,
    target_rec_ptr: XLogRecPtr,
    cur_page: &mut [u8],
) -> i32 {
    let _ = (state, target_page_ptr, req_len, target_rec_ptr, cur_page);
    unimplemented!()
}

pub fn read_local_xlog_page_no_wait(
    state: &mut XLogReaderState,
    target_page_ptr: XLogRecPtr,
    req_len: i32,
    target_rec_ptr: XLogRecPtr,
    cur_page: &mut [u8],
) -> i32 {
    let _ = (state, target_page_ptr, req_len, target_rec_ptr, cur_page);
    unimplemented!()
}

/// C threads the timeline back through `TimeLineID *tli_p`; returned here.
pub fn wal_segment_open(
    state: &mut XLogReaderState,
    next_seg_no: XLogSegNo,
) -> TimeLineID {
    let _ = (state, next_seg_no);
    unimplemented!()
}

pub fn wal_segment_close(state: &mut XLogReaderState) {
    let _ = state;
    unimplemented!()
}

pub fn XLogReadDetermineTimeline(
    state: &mut XLogReaderState,
    want_page: XLogRecPtr,
    want_length: u32,
    curr_tli: TimeLineID,
) {
    let _ = (state, want_page, want_length, curr_tli);
    unimplemented!()
}

pub fn WALReadRaiseError(errinfo: &WALReadError) {
    let _ = errinfo;
    unimplemented!()
}
