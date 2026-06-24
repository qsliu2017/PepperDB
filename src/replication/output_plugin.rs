//! Translated from PostgreSQL src/include/replication/output_plugin.h
//!
//! Logical decoding output plugin interface. The C `OutputPluginCallbacks`
//! routine struct (a vtable of fn pointers, NULL-checked at call sites) becomes
//! a Rust trait per routine-struct.md appendix B: required callbacks are trait
//! methods, optional ones are provided default methods (the runtime NULL check).

use crate::access::xlogdefs::{RepOriginId, XLogRecPtr};
use crate::c::TransactionId;
use crate::datatype::timestamp::TimestampTz;
use crate::replication::reorderbuffer::{ReorderBufferChange, ReorderBufferTXN};
use crate::utils::relcache::Relation;

// LogicalDecodingContext is private to logical.c; named opaquely here.
// TODO(struct-forward): repoint to crate::replication::logical::LogicalDecodingContext in Phase 2.
#[deprecated(
    note = "TODO(struct-forward): repoint to crate::replication::logical::LogicalDecodingContext in Phase 2"
)]
pub struct LogicalDecodingContext {
    _opaque: [u8; 0],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputPluginOutputType {
    OUTPUT_PLUGIN_BINARY_OUTPUT,
    OUTPUT_PLUGIN_TEXTUAL_OUTPUT,
}

/// Options set by the output plugin, in the startup callback.
#[derive(Debug, Clone, Copy)]
pub struct OutputPluginOptions {
    pub output_type: OutputPluginOutputType,
    pub receive_rewrites: bool,
}

/// Output plugin callbacks. Required callbacks are methods; the optional ones
/// (`filter_prepare_cb`, the two-phase and streaming groups) are provided
/// default methods, matching C's `if (cb != NULL)` runtime checks.
#[allow(deprecated)]
pub trait OutputPluginCallbacks {
    // --- required ---
    fn startup_cb(&self, ctx: &mut LogicalDecodingContext, options: &mut OutputPluginOptions, is_init: bool);
    fn begin_cb(&self, ctx: &mut LogicalDecodingContext, txn: &mut ReorderBufferTXN);
    fn change_cb(
        &self,
        ctx: &mut LogicalDecodingContext,
        txn: &mut ReorderBufferTXN,
        relation: Relation,
        change: &mut ReorderBufferChange,
    );
    fn truncate_cb(
        &self,
        ctx: &mut LogicalDecodingContext,
        txn: &mut ReorderBufferTXN,
        relations: &mut [Relation],
        change: &mut ReorderBufferChange,
    );
    fn commit_cb(&self, ctx: &mut LogicalDecodingContext, txn: &mut ReorderBufferTXN, commit_lsn: XLogRecPtr);
    fn message_cb(
        &self,
        ctx: &mut LogicalDecodingContext,
        txn: &mut ReorderBufferTXN,
        message_lsn: XLogRecPtr,
        transactional: bool,
        prefix: &str,
        message: &[u8],
    );
    fn shutdown_cb(&self, ctx: &mut LogicalDecodingContext);

    // --- optional: filter ---
    fn filter_by_origin_cb(&self, _ctx: &mut LogicalDecodingContext, _origin_id: RepOriginId) -> bool {
        false
    }

    // --- optional: two-phase commit ---
    fn filter_prepare_cb(&self, _ctx: &mut LogicalDecodingContext, _xid: TransactionId, _gid: &str) -> bool {
        false
    }
    fn begin_prepare_cb(&self, _ctx: &mut LogicalDecodingContext, _txn: &mut ReorderBufferTXN) {}
    fn prepare_cb(&self, _ctx: &mut LogicalDecodingContext, _txn: &mut ReorderBufferTXN, _prepare_lsn: XLogRecPtr) {}
    fn commit_prepared_cb(&self, _ctx: &mut LogicalDecodingContext, _txn: &mut ReorderBufferTXN, _commit_lsn: XLogRecPtr) {}
    fn rollback_prepared_cb(
        &self,
        _ctx: &mut LogicalDecodingContext,
        _txn: &mut ReorderBufferTXN,
        _prepare_end_lsn: XLogRecPtr,
        _prepare_time: TimestampTz,
    ) {
    }

    // --- optional: streaming of in-progress transactions ---
    fn stream_start_cb(&self, _ctx: &mut LogicalDecodingContext, _txn: &mut ReorderBufferTXN) {}
    fn stream_stop_cb(&self, _ctx: &mut LogicalDecodingContext, _txn: &mut ReorderBufferTXN) {}
    fn stream_abort_cb(&self, _ctx: &mut LogicalDecodingContext, _txn: &mut ReorderBufferTXN, _abort_lsn: XLogRecPtr) {}
    fn stream_prepare_cb(&self, _ctx: &mut LogicalDecodingContext, _txn: &mut ReorderBufferTXN, _prepare_lsn: XLogRecPtr) {}
    fn stream_commit_cb(&self, _ctx: &mut LogicalDecodingContext, _txn: &mut ReorderBufferTXN, _commit_lsn: XLogRecPtr) {}
    fn stream_change_cb(
        &self,
        _ctx: &mut LogicalDecodingContext,
        _txn: &mut ReorderBufferTXN,
        _relation: Relation,
        _change: &mut ReorderBufferChange,
    ) {
    }
    fn stream_message_cb(
        &self,
        _ctx: &mut LogicalDecodingContext,
        _txn: &mut ReorderBufferTXN,
        _message_lsn: XLogRecPtr,
        _transactional: bool,
        _prefix: &str,
        _message: &[u8],
    ) {
    }
    fn stream_truncate_cb(
        &self,
        _ctx: &mut LogicalDecodingContext,
        _txn: &mut ReorderBufferTXN,
        _relations: &mut [Relation],
        _change: &mut ReorderBufferChange,
    ) {
    }
}

/// Symbol type for `_PG_output_plugin_init`: a shared-library entry that fills in
/// the callback table. In Rust this is the plugin's registration of its
/// `OutputPluginCallbacks` impl.
// TODO(struct-forward): model plugin registration in Phase 2.
#[allow(deprecated)]
pub fn pg_output_plugin_init() -> Box<dyn OutputPluginCallbacks> {
    unimplemented!()
}

// Functions in replication/logical/logical.c
#[allow(deprecated)]
pub fn output_plugin_prepare_write(_ctx: &mut LogicalDecodingContext, _last_write: bool) {
    unimplemented!()
}
#[allow(deprecated)]
pub fn output_plugin_write(_ctx: &mut LogicalDecodingContext, _last_write: bool) {
    unimplemented!()
}
#[allow(deprecated)]
pub fn output_plugin_update_progress(_ctx: &mut LogicalDecodingContext, _skipped_xact: bool) {
    unimplemented!()
}
