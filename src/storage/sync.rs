//! Translated from PostgreSQL src/include/storage/sync.h
//!
//! File synchronization management.

use crate::common::relpath::ForkNumber;
use crate::storage::relfilelocator::RelFileLocator;

/// Type of sync request.
pub enum SyncRequestType {
    SyncRequest,       // schedule a call of sync function
    SyncUnlinkRequest, // schedule a call of unlink function
    SyncForgetRequest, // forget all calls for a tag
    SyncFilterRequest, // forget all calls satisfying match fn
}

/// Which set of functions to use to handle a given request. The values must
/// match the indexes of the function table in sync.c.
pub enum SyncRequestHandler {
    Md = 0,
    Clog,
    CommitTs,
    MultixactOffset,
    MultixactMember,
    None,
}

/// A tag identifying a file (in-memory). `handler`/`forknum` are kept narrow
/// (int16) to match md.c's space-saving usage.
pub struct FileTag {
    pub handler: i16, // SyncRequestHandler value
    pub forknum: i16, // ForkNumber value
    pub rlocator: RelFileLocator,
    pub segno: u64,
}

impl FileTag {
    pub fn handler(&self) -> SyncRequestHandler {
        match self.handler {
            0 => SyncRequestHandler::Md,
            1 => SyncRequestHandler::Clog,
            2 => SyncRequestHandler::CommitTs,
            3 => SyncRequestHandler::MultixactOffset,
            4 => SyncRequestHandler::MultixactMember,
            _ => SyncRequestHandler::None,
        }
    }
    pub fn forknum(&self) -> ForkNumber {
        match self.forknum {
            0 => ForkNumber::MAIN_FORKNUM,
            1 => ForkNumber::FSM_FORKNUM,
            2 => ForkNumber::VISIBILITYMAP_FORKNUM,
            3 => ForkNumber::INIT_FORKNUM,
            _ => ForkNumber::InvalidForkNumber,
        }
    }
}

pub fn InitSync() {
    unimplemented!()
}
pub fn SyncPreCheckpoint() {
    unimplemented!()
}
pub fn SyncPostCheckpoint() {
    unimplemented!()
}
pub fn ProcessSyncRequests() {
    unimplemented!()
}
pub fn RememberSyncRequest(_ftag: &FileTag, _type: SyncRequestType) {
    unimplemented!()
}
/// Returns true on success (false -> caller should retry / fall back).
pub fn RegisterSyncRequest(_ftag: &FileTag, _type: SyncRequestType, _retry_on_error: bool) -> bool {
    unimplemented!()
}
