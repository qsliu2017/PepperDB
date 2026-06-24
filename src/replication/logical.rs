//! Translated from PostgreSQL src/include/replication/logical.h
//! PostgreSQL logical decoding coordination.
//!
//! Defines the real `LogicalDecodingContext`, resolving the Phase-1 forward
//! declarations in `replication::output_plugin` and `access::xlog_internal`
//! (both repointed here in Phase 2).

use crate::access::xlogdefs::{RepOriginId, XLogRecPtr};
use crate::access::xlogreader::{XLogReaderRoutine, XLogReaderState};
use crate::c::TransactionId;
use crate::lib::stringinfo::StringInfo;
use crate::replication::output_plugin::{OutputPluginCallbacks, OutputPluginOptions};
use crate::replication::reorderbuffer::ReorderBuffer;
use crate::replication::slot::ReplicationSlot;
use crate::replication::snapbuild::SnapBuild;
use crate::utils::memutils::MemoryContext;

/// Writer callback: write/stream out decoded data for a transaction.
/// C `void (*)(LogicalDecodingContext*, XLogRecPtr, TransactionId, bool)`.
pub type LogicalOutputPluginWriterWrite =
    fn(lr: &mut LogicalDecodingContext, ptr: XLogRecPtr, xid: TransactionId, last_write: bool);

/// Same signature as the write callback; prepares the buffer for a write.
pub type LogicalOutputPluginWriterPrepareWrite = LogicalOutputPluginWriterWrite;

/// Progress-update callback. C `void (*)(LogicalDecodingContext*, XLogRecPtr,
/// TransactionId, bool skipped_xact)`.
pub type LogicalOutputPluginWriterUpdateProgress =
    fn(lr: &mut LogicalDecodingContext, ptr: XLogRecPtr, xid: TransactionId, skipped_xact: bool);

/// Logical decoding context (in-memory). This is the authoritative definition;
/// `output_plugin` and `xlog_internal` carry opaque placeholders for it.
#[allow(deprecated)] // SnapBuild placeholder, repointed in Phase 2
pub struct LogicalDecodingContext {
    /// memory context this is all allocated in
    pub context: MemoryContext,

    /// The associated replication slot
    pub slot: *mut ReplicationSlot, // TODO(ptr)

    /// infrastructure pieces for decoding
    pub reader: *mut XLogReaderState, // TODO(ptr)
    pub reorder: *mut ReorderBuffer,  // TODO(ptr)
    pub snapshot_builder: *mut SnapBuild, // TODO(ptr)

    /// Marks the context as fast-forward decoding (no plugin loaded; most of the
    /// following fields are then unused).
    pub fast_forward: bool,

    /// Output plugin vtable; the C value-typed `OutputPluginCallbacks` struct of
    /// fn pointers is the `OutputPluginCallbacks` trait here.
    pub callbacks: Box<dyn OutputPluginCallbacks>, // TODO(ptr)
    pub options: OutputPluginOptions,

    /// User-specified options.
    pub output_plugin_options: Vec<*mut core::ffi::c_void>, // C: List *; TODO(ptr)

    /// User-provided callbacks for writing/streaming out data.
    pub prepare_write: LogicalOutputPluginWriterPrepareWrite,
    pub write: LogicalOutputPluginWriterWrite,
    pub update_progress: LogicalOutputPluginWriterUpdateProgress,

    /// Output buffer.
    pub out: StringInfo,

    /// Private data pointer of the output plugin.
    pub output_plugin_private: *mut core::ffi::c_void, // TODO(ptr)

    /// Private data pointer for the data writer.
    pub output_writer_private: *mut core::ffi::c_void, // TODO(ptr)

    /// Does the output plugin support streaming, and is it enabled?
    pub streaming: bool,

    /// Does the output plugin support two-phase decoding, and is it enabled?
    pub twophase: bool,

    /// Is the two-phase option given by the output plugin?
    pub twophase_opt_given: bool,

    /// State for writing output.
    pub accept_writes: bool,
    pub prepared_write: bool,
    pub write_location: XLogRecPtr,
    pub write_xid: TransactionId,
    /// Are we processing the end LSN of a transaction?
    pub end_xact: bool,

    /// Do we need to process any change in fast_forward mode?
    pub processing_required: bool,
}

/// Callback context for logical decoding errors (`void *arg` -> closure later).
pub struct LogicalErrorCallbackState {
    pub ctx: *mut LogicalDecodingContext, // TODO(ptr)
    pub callback_name: String,
    pub report_location: XLogRecPtr,
}

pub fn CheckLogicalDecodingRequirements() {
    unimplemented!()
}

pub fn CreateInitDecodingContext(
    _plugin: &str,
    _output_plugin_options: Vec<*mut core::ffi::c_void>,
    _need_full_snapshot: bool,
    _restart_lsn: XLogRecPtr,
    _xl_routine: &dyn XLogReaderRoutine,
    _prepare_write: LogicalOutputPluginWriterPrepareWrite,
    _do_write: LogicalOutputPluginWriterWrite,
    _update_progress: LogicalOutputPluginWriterUpdateProgress,
) -> *mut LogicalDecodingContext {
    unimplemented!()
}

pub fn CreateDecodingContext(
    _start_lsn: XLogRecPtr,
    _output_plugin_options: Vec<*mut core::ffi::c_void>,
    _fast_forward: bool,
    _xl_routine: &dyn XLogReaderRoutine,
    _prepare_write: LogicalOutputPluginWriterPrepareWrite,
    _do_write: LogicalOutputPluginWriterWrite,
    _update_progress: LogicalOutputPluginWriterUpdateProgress,
) -> *mut LogicalDecodingContext {
    unimplemented!()
}

pub fn DecodingContextFindStartpoint(_ctx: &mut LogicalDecodingContext) {
    unimplemented!()
}

pub fn DecodingContextReady(_ctx: &mut LogicalDecodingContext) -> bool {
    unimplemented!()
}

pub fn FreeDecodingContext(_ctx: &mut LogicalDecodingContext) {
    unimplemented!()
}

pub fn LogicalIncreaseXminForSlot(_current_lsn: XLogRecPtr, _xmin: TransactionId) {
    unimplemented!()
}

pub fn LogicalIncreaseRestartDecodingForSlot(_current_lsn: XLogRecPtr, _restart_lsn: XLogRecPtr) {
    unimplemented!()
}

pub fn LogicalConfirmReceivedLocation(_lsn: XLogRecPtr) {
    unimplemented!()
}

pub fn filter_prepare_cb_wrapper(
    _ctx: &mut LogicalDecodingContext,
    _xid: TransactionId,
    _gid: &str,
) -> bool {
    unimplemented!()
}

pub fn filter_by_origin_cb_wrapper(
    _ctx: &mut LogicalDecodingContext,
    _origin_id: RepOriginId,
) -> bool {
    unimplemented!()
}

pub fn ResetLogicalStreamingState() {
    unimplemented!()
}

pub fn UpdateDecodingStats(_ctx: &mut LogicalDecodingContext) {
    unimplemented!()
}

pub fn LogicalReplicationSlotHasPendingWal(_end_of_wal: XLogRecPtr) -> bool {
    unimplemented!()
}

/// C fills `*found_consistent_snapshot` out-param -> returned alongside the LSN.
pub fn LogicalSlotAdvanceAndCheckSnapState(_moveto: XLogRecPtr) -> (XLogRecPtr, bool) {
    unimplemented!()
}
