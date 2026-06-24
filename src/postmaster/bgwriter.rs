//! Translated from PostgreSQL src/include/postmaster/bgwriter.h

use crate::c::Size;
use crate::storage::sync::{FileTag, SyncRequestType};

// GUC options (process-globals -> revisit as Session/GUC state in Phase 2).
pub static mut BgWriterDelay: i32 = 0;
pub static mut CheckPointTimeout: i32 = 0;
pub static mut CheckPointWarning: i32 = 0;
pub static mut CheckPointCompletionTarget: f64 = 0.0;

/// `BackgroundWriterMain` - process entry point, never returns.
pub fn BackgroundWriterMain(_startup_data: *const core::ffi::c_void, _startup_data_len: Size) -> ! {
    unimplemented!()
}

/// `CheckpointerMain` - process entry point, never returns.
pub fn CheckpointerMain(_startup_data: *const core::ffi::c_void, _startup_data_len: Size) -> ! {
    unimplemented!()
}

pub fn RequestCheckpoint(_flags: i32) {
    unimplemented!()
}

pub fn CheckpointWriteDelay(_flags: i32, _progress: f64) {
    unimplemented!()
}

pub fn ForwardSyncRequest(_ftag: &FileTag, _type_: SyncRequestType) -> bool {
    unimplemented!()
}

pub fn AbsorbSyncRequests() {
    unimplemented!()
}

pub fn CheckpointerShmemSize() -> Size {
    unimplemented!()
}

pub fn CheckpointerShmemInit() {
    unimplemented!()
}

pub fn FirstCallSinceLastCheckpoint() -> bool {
    unimplemented!()
}
