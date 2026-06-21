//! replication/output_plugin.h - PostgreSQL Logical Decode Plugin Interface

use std::ffi::{c_char, c_int, c_void};

use crate::access::transam::xlogdefs::{RepOriginId, XLogRecPtr};
use crate::c::{Size, TransactionId};
use crate::utils::rel::Relation;

// struct LogicalDecodingContext; (forward decl - defined in replication/logical.h, unported)
// TODO: dedup when replication/logical.h lands
pub type LogicalDecodingContext = c_void;

// ReorderBuffer types from replication/reorderbuffer.h (unported)
// TODO: dedup when replication/reorderbuffer.h lands
pub type ReorderBufferTXN = c_void;
pub type ReorderBufferChange = c_void;

// TimestampTz from datatype/timestamp.h (unported)
// TODO: dedup when datatype/timestamp.h lands
pub type TimestampTz = i64;

pub type OutputPluginOutputType = c_int;
pub const OUTPUT_PLUGIN_BINARY_OUTPUT: OutputPluginOutputType = 0;
pub const OUTPUT_PLUGIN_TEXTUAL_OUTPUT: OutputPluginOutputType = 1;

/*
 * Options set by the output plugin, in the startup callback.
 */
#[repr(C)]
pub struct OutputPluginOptions {
    pub output_type: OutputPluginOutputType,
    pub receive_rewrites: bool,
}

/*
 * Type of the shared library symbol _PG_output_plugin_init that is looked up
 * when loading an output plugin shared library.
 */
pub type LogicalOutputPluginInit =
    Option<unsafe extern "C" fn(cb: *mut OutputPluginCallbacks)>;

pub unsafe fn _PG_output_plugin_init(cb: *mut OutputPluginCallbacks) { crate::replication::pgoutput::pgoutput::_PG_output_plugin_init(cb as _) }

/*
 * Callback that gets called in a user-defined plugin. ctx->private_data can
 * be set to some private data.
 */
pub type LogicalDecodeStartupCB = Option<
    unsafe extern "C" fn(
        ctx: *mut LogicalDecodingContext,
        options: *mut OutputPluginOptions,
        is_init: bool,
    ),
>;

/*
 * Callback called for every (explicit or implicit) BEGIN of a successful
 * transaction.
 */
pub type LogicalDecodeBeginCB = Option<
    unsafe extern "C" fn(ctx: *mut LogicalDecodingContext, txn: *mut ReorderBufferTXN),
>;

/*
 * Callback for every individual change in a successful transaction.
 */
pub type LogicalDecodeChangeCB = Option<
    unsafe extern "C" fn(
        ctx: *mut LogicalDecodingContext,
        txn: *mut ReorderBufferTXN,
        relation: Relation,
        change: *mut ReorderBufferChange,
    ),
>;

/*
 * Callback for every TRUNCATE in a successful transaction.
 */
pub type LogicalDecodeTruncateCB = Option<
    unsafe extern "C" fn(
        ctx: *mut LogicalDecodingContext,
        txn: *mut ReorderBufferTXN,
        nrelations: c_int,
        relations: *mut Relation,
        change: *mut ReorderBufferChange,
    ),
>;

/*
 * Called for every (explicit or implicit) COMMIT of a successful transaction.
 */
pub type LogicalDecodeCommitCB = Option<
    unsafe extern "C" fn(
        ctx: *mut LogicalDecodingContext,
        txn: *mut ReorderBufferTXN,
        commit_lsn: XLogRecPtr,
    ),
>;

/*
 * Called for the generic logical decoding messages.
 */
pub type LogicalDecodeMessageCB = Option<
    unsafe extern "C" fn(
        ctx: *mut LogicalDecodingContext,
        txn: *mut ReorderBufferTXN,
        message_lsn: XLogRecPtr,
        transactional: bool,
        prefix: *const c_char,
        message_size: Size,
        message: *const c_char,
    ),
>;

/*
 * Filter changes by origin.
 */
pub type LogicalDecodeFilterByOriginCB = Option<
    unsafe extern "C" fn(ctx: *mut LogicalDecodingContext, origin_id: RepOriginId) -> bool,
>;

/*
 * Called to shutdown an output plugin.
 */
pub type LogicalDecodeShutdownCB =
    Option<unsafe extern "C" fn(ctx: *mut LogicalDecodingContext)>;

/*
 * Called before decoding of PREPARE record to decide whether this
 * transaction should be decoded with separate calls to prepare and
 * commit_prepared/rollback_prepared callbacks or wait till COMMIT PREPARED
 * and sent as usual transaction.
 */
pub type LogicalDecodeFilterPrepareCB = Option<
    unsafe extern "C" fn(
        ctx: *mut LogicalDecodingContext,
        xid: TransactionId,
        gid: *const c_char,
    ) -> bool,
>;

/*
 * Callback called for every BEGIN of a prepared transaction.
 */
pub type LogicalDecodeBeginPrepareCB = Option<
    unsafe extern "C" fn(ctx: *mut LogicalDecodingContext, txn: *mut ReorderBufferTXN),
>;

/*
 * Called for PREPARE record unless it was filtered by filter_prepare()
 * callback.
 */
pub type LogicalDecodePrepareCB = Option<
    unsafe extern "C" fn(
        ctx: *mut LogicalDecodingContext,
        txn: *mut ReorderBufferTXN,
        prepare_lsn: XLogRecPtr,
    ),
>;

/*
 * Called for COMMIT PREPARED.
 */
pub type LogicalDecodeCommitPreparedCB = Option<
    unsafe extern "C" fn(
        ctx: *mut LogicalDecodingContext,
        txn: *mut ReorderBufferTXN,
        commit_lsn: XLogRecPtr,
    ),
>;

/*
 * Called for ROLLBACK PREPARED.
 */
pub type LogicalDecodeRollbackPreparedCB = Option<
    unsafe extern "C" fn(
        ctx: *mut LogicalDecodingContext,
        txn: *mut ReorderBufferTXN,
        prepare_end_lsn: XLogRecPtr,
        prepare_time: TimestampTz,
    ),
>;

/*
 * Called when starting to stream a block of changes from in-progress
 * transaction (may be called repeatedly, if it's streamed in multiple
 * chunks).
 */
pub type LogicalDecodeStreamStartCB = Option<
    unsafe extern "C" fn(ctx: *mut LogicalDecodingContext, txn: *mut ReorderBufferTXN),
>;

/*
 * Called when stopping to stream a block of changes from in-progress
 * transaction to a remote node (may be called repeatedly, if it's streamed
 * in multiple chunks).
 */
pub type LogicalDecodeStreamStopCB = Option<
    unsafe extern "C" fn(ctx: *mut LogicalDecodingContext, txn: *mut ReorderBufferTXN),
>;

/*
 * Called to discard changes streamed to remote node from in-progress
 * transaction.
 */
pub type LogicalDecodeStreamAbortCB = Option<
    unsafe extern "C" fn(
        ctx: *mut LogicalDecodingContext,
        txn: *mut ReorderBufferTXN,
        abort_lsn: XLogRecPtr,
    ),
>;

/*
 * Called to prepare changes streamed to remote node from in-progress
 * transaction. This is called as part of a two-phase commit.
 */
pub type LogicalDecodeStreamPrepareCB = Option<
    unsafe extern "C" fn(
        ctx: *mut LogicalDecodingContext,
        txn: *mut ReorderBufferTXN,
        prepare_lsn: XLogRecPtr,
    ),
>;

/*
 * Called to apply changes streamed to remote node from in-progress
 * transaction.
 */
pub type LogicalDecodeStreamCommitCB = Option<
    unsafe extern "C" fn(
        ctx: *mut LogicalDecodingContext,
        txn: *mut ReorderBufferTXN,
        commit_lsn: XLogRecPtr,
    ),
>;

/*
 * Callback for streaming individual changes from in-progress transactions.
 */
pub type LogicalDecodeStreamChangeCB = Option<
    unsafe extern "C" fn(
        ctx: *mut LogicalDecodingContext,
        txn: *mut ReorderBufferTXN,
        relation: Relation,
        change: *mut ReorderBufferChange,
    ),
>;

/*
 * Callback for streaming generic logical decoding messages from in-progress
 * transactions.
 */
pub type LogicalDecodeStreamMessageCB = Option<
    unsafe extern "C" fn(
        ctx: *mut LogicalDecodingContext,
        txn: *mut ReorderBufferTXN,
        message_lsn: XLogRecPtr,
        transactional: bool,
        prefix: *const c_char,
        message_size: Size,
        message: *const c_char,
    ),
>;

/*
 * Callback for streaming truncates from in-progress transactions.
 */
pub type LogicalDecodeStreamTruncateCB = Option<
    unsafe extern "C" fn(
        ctx: *mut LogicalDecodingContext,
        txn: *mut ReorderBufferTXN,
        nrelations: c_int,
        relations: *mut Relation,
        change: *mut ReorderBufferChange,
    ),
>;

/*
 * Output plugin callbacks
 */
#[repr(C)]
pub struct OutputPluginCallbacks {
    pub startup_cb: LogicalDecodeStartupCB,
    pub begin_cb: LogicalDecodeBeginCB,
    pub change_cb: LogicalDecodeChangeCB,
    pub truncate_cb: LogicalDecodeTruncateCB,
    pub commit_cb: LogicalDecodeCommitCB,
    pub message_cb: LogicalDecodeMessageCB,
    pub filter_by_origin_cb: LogicalDecodeFilterByOriginCB,
    pub shutdown_cb: LogicalDecodeShutdownCB,

    /* streaming of changes at prepare time */
    pub filter_prepare_cb: LogicalDecodeFilterPrepareCB,
    pub begin_prepare_cb: LogicalDecodeBeginPrepareCB,
    pub prepare_cb: LogicalDecodePrepareCB,
    pub commit_prepared_cb: LogicalDecodeCommitPreparedCB,
    pub rollback_prepared_cb: LogicalDecodeRollbackPreparedCB,

    /* streaming of changes */
    pub stream_start_cb: LogicalDecodeStreamStartCB,
    pub stream_stop_cb: LogicalDecodeStreamStopCB,
    pub stream_abort_cb: LogicalDecodeStreamAbortCB,
    pub stream_prepare_cb: LogicalDecodeStreamPrepareCB,
    pub stream_commit_cb: LogicalDecodeStreamCommitCB,
    pub stream_change_cb: LogicalDecodeStreamChangeCB,
    pub stream_message_cb: LogicalDecodeStreamMessageCB,
    pub stream_truncate_cb: LogicalDecodeStreamTruncateCB,
}

/* Functions in replication/logical/logical.c */
pub unsafe fn OutputPluginPrepareWrite(ctx: *mut LogicalDecodingContext, last_write: bool) { crate::replication::logical::logical::OutputPluginPrepareWrite(ctx as _, last_write) }

pub unsafe fn OutputPluginWrite(ctx: *mut LogicalDecodingContext, last_write: bool) { crate::replication::logical::logical::OutputPluginWrite(ctx as _, last_write) }

pub unsafe fn OutputPluginUpdateProgress(ctx: *mut LogicalDecodingContext, skipped_xact: bool) { crate::replication::logical::logical::OutputPluginUpdateProgress(ctx as _, skipped_xact) }
