//! Translated from PostgreSQL src/include/replication/origin.h
//!
//! Exports from replication/logical/origin.c -- replication origin tracking.

use crate::access::xlogdefs::{RepOriginId, XLogRecPtr};
use crate::access::xlogreader::XLogReaderState;
use crate::datatype::timestamp::TimestampTz;

/// On-disk WAL record: set a replication origin's progress.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct xl_replorigin_set {
    pub remote_lsn: XLogRecPtr,
    pub node_id: RepOriginId,
    pub force: bool,
}
const _: () = assert!(core::mem::offset_of!(xl_replorigin_set, remote_lsn) == 0);
const _: () = assert!(core::mem::offset_of!(xl_replorigin_set, node_id) == 8);

/// On-disk WAL record: drop a replication origin.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct xl_replorigin_drop {
    pub node_id: RepOriginId,
}

/* WAL opcodes (xl_info nibble, kept raw) */
pub const XLOG_REPLORIGIN_SET: u8 = 0x00;
pub const XLOG_REPLORIGIN_DROP: u8 = 0x10;

pub const InvalidRepOriginId: RepOriginId = RepOriginId(0);
pub const DoNotReplicateId: RepOriginId = RepOriginId(u16::MAX);

/// To avoid needing a TOAST table for pg_replication_origin, origin names are
/// limited to 512 bytes.
pub const MAX_RONAME_LEN: usize = 512;

/* Session state (was process-global PGDLLIMPORT vars; threading deferred). */
// TODO(global): move replorigin_session_* to a Session/task-local context.
pub static mut replorigin_session_origin: RepOriginId = InvalidRepOriginId;
pub static mut replorigin_session_origin_lsn: XLogRecPtr = XLogRecPtr(0);
pub static mut replorigin_session_origin_timestamp: TimestampTz = 0;

/* GUCs */
pub static mut max_active_replication_origins: i32 = 0;

/* API for querying & manipulating replication origins */

/// InvalidRepOriginId (with missing_ok) -> None.
pub fn replorigin_by_name(_roname: &str, _missing_ok: bool) -> Option<RepOriginId> {
    unimplemented!()
}
pub fn replorigin_create(_roname: &str) -> RepOriginId {
    unimplemented!()
}
pub fn replorigin_drop_by_name(_name: &str, _missing_ok: bool, _nowait: bool) {
    unimplemented!()
}
/// C: `bool replorigin_by_oid(roident, missing_ok, char **roname)` -- the bool
/// success + name out-param collapse to `Option<String>` (None = not found).
pub fn replorigin_by_oid(_roident: RepOriginId, _missing_ok: bool) -> Option<String> {
    unimplemented!()
}

/* API for querying & manipulating replication progress tracking */
pub fn replorigin_advance(
    _node: RepOriginId,
    _remote_commit: XLogRecPtr,
    _local_commit: XLogRecPtr,
    _go_backward: bool,
    _wal_log: bool,
) {
    unimplemented!()
}
pub fn replorigin_get_progress(_node: RepOriginId, _flush: bool) -> XLogRecPtr {
    unimplemented!()
}

pub fn replorigin_session_advance(_remote_commit: XLogRecPtr, _local_commit: XLogRecPtr) {
    unimplemented!()
}
pub fn replorigin_session_setup(_node: RepOriginId, _acquired_by: i32) {
    unimplemented!()
}
pub fn replorigin_session_reset() {
    unimplemented!()
}
pub fn replorigin_session_get_progress(_flush: bool) -> XLogRecPtr {
    unimplemented!()
}

/* Checkpoint/Startup integration */
pub fn CheckPointReplicationOrigin() {
    unimplemented!()
}
pub fn StartupReplicationOrigin() {
    unimplemented!()
}

/* WAL logging */
pub fn replorigin_redo(_record: &mut XLogReaderState) {
    unimplemented!()
}
pub fn replorigin_desc(_buf: &mut String, _record: &mut XLogReaderState) {
    unimplemented!()
}
pub fn replorigin_identify(_info: u8) -> &'static str {
    unimplemented!()
}

/* shared memory allocation (single-process: heap state; sizing is a no-op stub) */
pub fn ReplicationOriginShmemSize() -> usize {
    unimplemented!()
}
pub fn ReplicationOriginShmemInit() {
    unimplemented!()
}
