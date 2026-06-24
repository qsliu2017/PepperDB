//! Translated from PostgreSQL src/include/access/xlogprefetcher.h

use crate::access::xlogdefs::XLogRecPtr;
use crate::access::xlogrecord::XLogRecord;
use crate::access::xlogreader::XLogReaderState;

// GUC. TODO(global-state): move to a threaded Session.
pub static mut recovery_prefetch: i32 = 0;

/// Possible values for recovery_prefetch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum RecoveryPrefetchValue {
    Off,
    On,
    Try,
}

/// Opaque recovery prefetcher state (definition private to xlogprefetcher.c).
pub struct XLogPrefetcher {
    _private: [u8; 0],
}

pub fn XLogPrefetchReconfigure() {
    unimplemented!()
}
pub fn XLogPrefetchShmemSize() -> usize {
    unimplemented!()
}
pub fn XLogPrefetchShmemInit() {
    unimplemented!()
}
pub fn XLogPrefetchResetStats() {
    unimplemented!()
}
pub fn XLogPrefetcherAllocate(_reader: &mut XLogReaderState) -> *mut XLogPrefetcher {
    unimplemented!()
}
pub fn XLogPrefetcherFree(_prefetcher: *mut XLogPrefetcher) {
    unimplemented!()
}
pub fn XLogPrefetcherGetReader(_prefetcher: *mut XLogPrefetcher) -> *mut XLogReaderState {
    unimplemented!()
}
pub fn XLogPrefetcherBeginRead(_prefetcher: *mut XLogPrefetcher, _recPtr: XLogRecPtr) {
    unimplemented!()
}
/// Reads the next record. Ok(None) = end of WAL; Err(msg) = read error.
pub fn XLogPrefetcherReadRecord(
    _prefetcher: *mut XLogPrefetcher,
) -> Result<Option<*mut XLogRecord>, String> {
    unimplemented!()
}
pub fn XLogPrefetcherComputeStats(_prefetcher: *mut XLogPrefetcher) {
    unimplemented!()
}
