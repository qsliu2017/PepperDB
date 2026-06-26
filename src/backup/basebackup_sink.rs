//! Translated from PostgreSQL src/include/backup/basebackup_sink.h
//! API for filtering or sending base-backup archives to a final destination.
//!
//! A `bbsink` is a chain of sinks, each forwarding (possibly modified) callbacks
//! to the next. The C `bbsink_ops` vtable -> a `BbsinkOps` trait (routine-struct.md);
//! all callbacks are required, the forwarding sink calls into `next`.

use crate::access::xlogdefs::{TimeLineID, XLogRecPtr};
use crate::backup::basebackup::TablespaceInfo;
use crate::common::compression::PgCompressSpecification;

/// Overall backup state shared by all bbsink objects for a backup.
pub struct BbsinkState {
    pub tablespaces: Vec<TablespaceInfo>, // list of tablespaceinfo objects
    pub tablespace_num: i32,
    pub bytes_done: u64,
    pub bytes_total: u64,
    pub bytes_total_is_valid: bool,
    pub startptr: XLogRecPtr,
    pub starttli: TimeLineID,
}

/// Common data for any type of basebackup sink. The C `bbs_ops` vtable field is
/// replaced by the `BbsinkOps` trait impl; `next` is the forwarded-to sink.
pub struct Bbsink {
    pub buffer: Vec<u8>, // buffer for data destined for the sink (multiple of BLCKSZ)
    pub buffer_length: usize,
    pub next: Option<Box<Self>>, // sink we forward operations to
    pub state: Option<Box<BbsinkState>>,
}

/// Callbacks for a base backup sink. All callbacks are required; a sink that only
/// forwards uses the `bbsink_forward_*` free functions as its implementation.
pub trait BbsinkOps {
    /// Invoked once at the very start; must point `buffer` at writable storage.
    fn begin_backup(sink: &mut Bbsink);
    fn begin_archive(sink: &mut Bbsink, archive_name: &str);
    fn archive_contents(sink: &mut Bbsink, len: usize);
    fn end_archive(sink: &mut Bbsink);
    fn begin_manifest(sink: &mut Bbsink);
    fn manifest_contents(sink: &mut Bbsink, len: usize);
    fn end_manifest(sink: &mut Bbsink);
    fn end_backup(sink: &mut Bbsink, endptr: XLogRecPtr, endtli: TimeLineID);
    /// Release resources before destruction (also runs on error abort).
    fn cleanup(sink: &mut Bbsink);
}

/// Begin a backup.
pub fn bbsink_begin_backup(_sink: &mut Bbsink, _state: BbsinkState, _buffer_length: i32) {
    unimplemented!()
}

/// Begin an archive.
pub fn bbsink_begin_archive(_sink: &mut Bbsink, _archive_name: &str) {
    unimplemented!()
}

/// Process some of the contents of an archive.
pub fn bbsink_archive_contents(_sink: &mut Bbsink, _len: usize) {
    unimplemented!()
}

/// Finish an archive.
pub fn bbsink_end_archive(_sink: &mut Bbsink) {
    unimplemented!()
}

/// Begin the backup manifest.
pub fn bbsink_begin_manifest(_sink: &mut Bbsink) {
    unimplemented!()
}

/// Process some of the manifest contents.
pub fn bbsink_manifest_contents(_sink: &mut Bbsink, _len: usize) {
    unimplemented!()
}

/// Finish the backup manifest.
pub fn bbsink_end_manifest(_sink: &mut Bbsink) {
    unimplemented!()
}

/// Finish a backup.
pub fn bbsink_end_backup(_sink: &mut Bbsink, _endptr: XLogRecPtr, _endtli: TimeLineID) {
    unimplemented!()
}

/// Release resources before destruction.
pub fn bbsink_cleanup(_sink: &mut Bbsink) {
    unimplemented!()
}

/* Forwarding callbacks. Use these to pass operations through to next sink. */
pub fn bbsink_forward_begin_backup(_sink: &mut Bbsink) {
    unimplemented!()
}
pub fn bbsink_forward_begin_archive(_sink: &mut Bbsink, _archive_name: &str) {
    unimplemented!()
}
pub fn bbsink_forward_archive_contents(_sink: &mut Bbsink, _len: usize) {
    unimplemented!()
}
pub fn bbsink_forward_end_archive(_sink: &mut Bbsink) {
    unimplemented!()
}
pub fn bbsink_forward_begin_manifest(_sink: &mut Bbsink) {
    unimplemented!()
}
pub fn bbsink_forward_manifest_contents(_sink: &mut Bbsink, _len: usize) {
    unimplemented!()
}
pub fn bbsink_forward_end_manifest(_sink: &mut Bbsink) {
    unimplemented!()
}
pub fn bbsink_forward_end_backup(_sink: &mut Bbsink, _endptr: XLogRecPtr, _endtli: TimeLineID) {
    unimplemented!()
}
pub fn bbsink_forward_cleanup(_sink: &mut Bbsink) {
    unimplemented!()
}

/* Constructors for various types of sinks. */
pub fn bbsink_copystream_new(_send_to_client: bool) -> Bbsink {
    unimplemented!()
}
pub fn bbsink_gzip_new(_next: Bbsink, _spec: &PgCompressSpecification) -> Bbsink {
    unimplemented!()
}
pub fn bbsink_lz4_new(_next: Bbsink, _spec: &PgCompressSpecification) -> Bbsink {
    unimplemented!()
}
pub fn bbsink_zstd_new(_next: Bbsink, _spec: &PgCompressSpecification) -> Bbsink {
    unimplemented!()
}
pub fn bbsink_progress_new(_next: Bbsink, _estimate_backup_size: bool) -> Bbsink {
    unimplemented!()
}
pub fn bbsink_server_new(_next: Bbsink, _pathname: &str) -> Bbsink {
    unimplemented!()
}
pub fn bbsink_throttle_new(_next: Bbsink, _maxrate: u32) -> Bbsink {
    unimplemented!()
}

/* Extra interface functions for progress reporting. */
pub fn basebackup_progress_wait_checkpoint() {
    unimplemented!()
}
pub fn basebackup_progress_estimate_backup_size() {
    unimplemented!()
}
pub fn basebackup_progress_wait_wal_archive(_state: &mut BbsinkState) {
    unimplemented!()
}
pub fn basebackup_progress_transfer_wal() {
    unimplemented!()
}
pub fn basebackup_progress_done() {
    unimplemented!()
}
