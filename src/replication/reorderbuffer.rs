//! Translated from PostgreSQL src/include/replication/reorderbuffer.h
//!
//! PostgreSQL logical replay/reorder buffer management. Large in-memory structs
//! plus API stubs. Intrusive `dlist`/`dclist` links become owned collections;
//! `HTAB` -> `HashMap`; `pairingheap` -> `crate::lib::pairingheap`.

use std::collections::HashMap;

use crate::access::xlogdefs::{RepOriginId, XLogRecPtr};
use crate::c::{CommandId, TransactionId};
use crate::datatype::timestamp::TimestampTz;
use crate::nodes::memnodes::MemoryContext;
use crate::postgres_ext::Oid;
use crate::storage::itemptr::ItemPointerData;
use crate::storage::relfilelocator::RelFileLocator;
use crate::storage::sinval::SharedInvalidationMessage;
use crate::utils::relcache::Relation;
use crate::utils::snapshot::SnapshotData;

use bitflags::bitflags;

/// paths for logical decoding data (relative to installation's $PGDATA)
pub const PG_LOGICAL_DIR: &str = "pg_logical";
pub const PG_LOGICAL_MAPPINGS_DIR: &str = "pg_logical/mappings";
pub const PG_LOGICAL_SNAPSHOTS_DIR: &str = "pg_logical/snapshots";

/* GUC variables (were process-globals). TODO(global). */
pub static mut logical_decoding_work_mem: i32 = 0;
pub static mut debug_logical_replication_streaming: i32 = 0;

/// possible values for debug_logical_replication_streaming
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DebugLogicalRepStreamingMode {
    Buffered,
    Immediate,
}

/// Types of the change passed to a 'change' callback. Sequential ordinal -> enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ReorderBufferChangeType {
    Insert,
    Update,
    Delete,
    Message,
    Invalidation,
    InternalSnapshot,
    InternalCommandId,
    InternalTupleCid,
    InternalSpecInsert,
    InternalSpecConfirm,
    InternalSpecAbort,
    Truncate,
}

/// A snapshot owned by a change/txn. C uses `Snapshot` (a pointer); modeled as an
/// owned optional here to avoid threading lifetimes through these in-memory structs.
pub type OwnedSnapshot = Option<Box<SnapshotData>>;

/// Per-change data; the C `union data` keyed by `action`.
pub enum ReorderBufferChangeData {
    /// Old/new tuples for INSERT|UPDATE|DELETE.
    Tp {
        rlocator: RelFileLocator,
        clear_toast_afterwards: bool,
        oldtuple: Option<Box<HeapTupleData>>, // valid for DELETE || UPDATE
        newtuple: Option<Box<HeapTupleData>>, // valid for INSERT || UPDATE
    },
    /// Truncate: one set of relations to be truncated.
    Truncate {
        cascade: bool,
        restart_seqs: bool,
        relids: Vec<Oid>,
    },
    /// Message with arbitrary data.
    Msg {
        prefix: String,
        message: Vec<u8>,
    },
    /// New snapshot (INTERNAL_SNAPSHOT).
    Snapshot(OwnedSnapshot),
    /// New command id for an existing snapshot (INTERNAL_COMMAND_ID).
    CommandId(CommandId),
    /// New cid mapping for a catalog-changing txn (INTERNAL_TUPLECID).
    TupleCid {
        locator: RelFileLocator,
        tid: ItemPointerData,
        cmin: CommandId,
        cmax: CommandId,
        combocid: CommandId,
    },
    /// Invalidation messages.
    Inval(Vec<SharedInvalidationMessage>),
}

/// A single 'change' (insert/update/delete/etc.). In-memory.
pub struct ReorderBufferChange {
    pub lsn: XLogRecPtr,
    pub action: ReorderBufferChangeType,
    /// transaction this change belongs to (was `struct ReorderBufferTXN *`).
    // TODO(ptr): owning vs borrowing of the txn link is not clear from the header.
    pub txn: Option<Box<ReorderBufferTXN>>,
    pub origin_id: RepOriginId,
    pub data: ReorderBufferChangeData,
    // dlist_node node -- linkage tracked by the owning txn's `changes` list.
}

bitflags! {
    /// ReorderBufferTXN txn_flags (single-bit set; composite mask kept).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct RbTxnFlags: u32 {
        const HAS_CATALOG_CHANGES   = 0x0001;
        const IS_SUBXACT            = 0x0002;
        const IS_SERIALIZED         = 0x0004;
        const IS_SERIALIZED_CLEAR   = 0x0008;
        const IS_STREAMED           = 0x0010;
        const HAS_PARTIAL_CHANGE    = 0x0020;
        const IS_PREPARED           = 0x0040;
        const SKIPPED_PREPARE       = 0x0080;
        const HAS_STREAMABLE_CHANGE = 0x0100;
        const SENT_PREPARE          = 0x0200;
        const IS_COMMITTED          = 0x0400;
        const IS_ABORTED            = 0x0800;
        const DISTR_INVAL_OVERFLOWED = 0x1000;

        const PREPARE_STATUS_MASK =
            Self::IS_PREPARED.bits() | Self::SKIPPED_PREPARE.bits() | Self::SENT_PREPARE.bits();
    }
}

/// A reassembled (sub)transaction. Large in-memory struct.
pub struct ReorderBufferTXN {
    pub txn_flags: RbTxnFlags,
    /// the transaction's xid (toplevel or sub).
    pub xid: TransactionId,
    /// xid of top-level transaction, if known.
    pub toplevel_xid: TransactionId,
    /// global transaction id (prepared transactions).
    pub gid: Option<String>,
    pub first_lsn: XLogRecPtr,
    pub final_lsn: XLogRecPtr,
    pub end_lsn: XLogRecPtr,
    /// toplevel transaction for this subxact (None for top-level).
    // TODO(ptr): the C `toptxn` is a back-pointer; ownership unclear from header.
    pub toptxn: Option<Box<ReorderBufferTXN>>,
    pub restart_decoding_lsn: XLogRecPtr,
    pub origin_id: RepOriginId,
    pub origin_lsn: XLogRecPtr,
    /// commit/prepare/abort time (C union of equal TimestampTz members).
    pub xact_time: TimestampTz,
    pub base_snapshot: OwnedSnapshot,
    pub base_snapshot_lsn: XLogRecPtr,
    // dlist_node base_snapshot_node -- link in txns_by_base_snapshot_lsn.
    pub snapshot_now: OwnedSnapshot,
    pub command_id: CommandId,
    /// # of changes in this txn (subxact changes tracked separately).
    pub nentries: u64,
    /// # of the above held in memory (vs spilled to disk).
    pub nentries_mem: u64,
    /// list of changes (was dlist_head).
    pub changes: Vec<ReorderBufferChange>,
    /// (relation, ctid) => (cmin, cmax) mappings (was dlist_head).
    pub tuplecids: Vec<ReorderBufferChange>,
    pub ntuplecids: u64,
    /// on-demand hash for tuplecids (was HTAB *).
    pub tuplecid_hash: Option<HashMap<u64, ReorderBufferTupleCidEnt>>,
    /// (potentially partial) toast entries (was HTAB *).
    pub toast_hash: Option<HashMap<Oid, ()>>,
    /// non-aborted subtransactions, toplevel only (was dlist_head).
    pub subtxns: Vec<ReorderBufferTXN>,
    pub nsubtxns: u32,
    /// stored cache invalidations (gathered at once).
    pub invalidations: Vec<SharedInvalidationMessage>,
    /// invalidations distributed by other transactions.
    pub invalidations_distributed: Vec<SharedInvalidationMessage>,
    // dlist_node node -- link in subtxns or toplevel list.
    // dlist_node catchange_node -- link in catalog-modifying txns.
    // pairingheap_node txn_node -- node in txn_heap.
    /// size of this transaction (in-memory changes, bytes).
    pub size: usize,
    /// size of top-transaction including sub-transactions.
    pub total_size: usize,
    /// private data pointer of the output plugin (void * -> opaque).
    // TODO(ptr): output-plugin-owned; closure/enum modeling deferred.
    pub output_plugin_private: Option<Box<()>>,
}

/// (relation, ctid) => (cmin, cmax) hash entry for catalog tuples.
/// Forward-referenced from reorderbuffer.c internals; modeled minimally here.
pub struct ReorderBufferTupleCidEnt {
    pub cmin: CommandId,
    pub cmax: CommandId,
    pub combocid: CommandId,
}

/// Forward decl: HeapTuple's owned tuple data. Real definition in access/htup.h.
// TODO(struct-forward): repoint to crate::access::htup::HeapTupleData in Phase 2.
#[deprecated(note = "TODO(struct-forward): repoint to crate::access::htup::HeapTupleData in Phase 2")]
pub struct HeapTupleData;

/* --- Callback signatures (the ReorderBuffer holds these as a vtable). --- */

pub type ReorderBufferApplyChangeCB =
    fn(rb: &mut ReorderBuffer, txn: &mut ReorderBufferTXN, relation: Relation, change: &mut ReorderBufferChange);
pub type ReorderBufferApplyTruncateCB = fn(
    rb: &mut ReorderBuffer,
    txn: &mut ReorderBufferTXN,
    relations: &mut [Relation],
    change: &mut ReorderBufferChange,
);
pub type ReorderBufferBeginCB = fn(rb: &mut ReorderBuffer, txn: &mut ReorderBufferTXN);
pub type ReorderBufferCommitCB =
    fn(rb: &mut ReorderBuffer, txn: &mut ReorderBufferTXN, commit_lsn: XLogRecPtr);
pub type ReorderBufferMessageCB = fn(
    rb: &mut ReorderBuffer,
    txn: &mut ReorderBufferTXN,
    message_lsn: XLogRecPtr,
    transactional: bool,
    prefix: &str,
    message: &[u8],
);
pub type ReorderBufferPrepareCB =
    fn(rb: &mut ReorderBuffer, txn: &mut ReorderBufferTXN, prepare_lsn: XLogRecPtr);
pub type ReorderBufferCommitPreparedCB =
    fn(rb: &mut ReorderBuffer, txn: &mut ReorderBufferTXN, commit_lsn: XLogRecPtr);
pub type ReorderBufferRollbackPreparedCB = fn(
    rb: &mut ReorderBuffer,
    txn: &mut ReorderBufferTXN,
    prepare_end_lsn: XLogRecPtr,
    prepare_time: TimestampTz,
);
pub type ReorderBufferStreamStartCB =
    fn(rb: &mut ReorderBuffer, txn: &mut ReorderBufferTXN, first_lsn: XLogRecPtr);
pub type ReorderBufferStreamStopCB =
    fn(rb: &mut ReorderBuffer, txn: &mut ReorderBufferTXN, last_lsn: XLogRecPtr);
pub type ReorderBufferStreamAbortCB =
    fn(rb: &mut ReorderBuffer, txn: &mut ReorderBufferTXN, abort_lsn: XLogRecPtr);
pub type ReorderBufferStreamPrepareCB =
    fn(rb: &mut ReorderBuffer, txn: &mut ReorderBufferTXN, prepare_lsn: XLogRecPtr);
pub type ReorderBufferStreamCommitCB =
    fn(rb: &mut ReorderBuffer, txn: &mut ReorderBufferTXN, commit_lsn: XLogRecPtr);
pub type ReorderBufferStreamChangeCB =
    fn(rb: &mut ReorderBuffer, txn: &mut ReorderBufferTXN, relation: Relation, change: &mut ReorderBufferChange);
pub type ReorderBufferStreamMessageCB = fn(
    rb: &mut ReorderBuffer,
    txn: &mut ReorderBufferTXN,
    message_lsn: XLogRecPtr,
    transactional: bool,
    prefix: &str,
    message: &[u8],
);
pub type ReorderBufferStreamTruncateCB = fn(
    rb: &mut ReorderBuffer,
    txn: &mut ReorderBufferTXN,
    relations: &mut [Relation],
    change: &mut ReorderBufferChange,
);
pub type ReorderBufferUpdateProgressTxnCB =
    fn(rb: &mut ReorderBuffer, txn: &mut ReorderBufferTXN, lsn: XLogRecPtr);

/// The reorder buffer itself. Large in-memory struct + callback vtable.
pub struct ReorderBuffer {
    /// xid => ReorderBufferTXN lookup (was HTAB *).
    pub by_txn: HashMap<TransactionId, ReorderBufferTXN>,
    /// possible toplevel xacts, ordered by first-record LSN (was dlist_head).
    pub toplevel_by_lsn: Vec<TransactionId>,
    /// txns/subtxns with a base snapshot, ordered by LSN (was dlist_head).
    pub txns_by_base_snapshot_lsn: Vec<TransactionId>,
    /// txns/subtxns that modified system catalogs (was dclist_head).
    pub catchange_txns: Vec<TransactionId>,

    /// one-entry cache for by_txn.
    pub by_txn_last_xid: TransactionId,
    // ReorderBufferTXN *by_txn_last_txn -- cached lookup; omitted from owned model.

    /* Commit-time callbacks. */
    pub begin: Option<ReorderBufferBeginCB>,
    pub apply_change: Option<ReorderBufferApplyChangeCB>,
    pub apply_truncate: Option<ReorderBufferApplyTruncateCB>,
    pub commit: Option<ReorderBufferCommitCB>,
    pub message: Option<ReorderBufferMessageCB>,

    /* Prepare-time callbacks. */
    pub begin_prepare: Option<ReorderBufferBeginCB>,
    pub prepare: Option<ReorderBufferPrepareCB>,
    pub commit_prepared: Option<ReorderBufferCommitPreparedCB>,
    pub rollback_prepared: Option<ReorderBufferRollbackPreparedCB>,

    /* Streaming callbacks. */
    pub stream_start: Option<ReorderBufferStreamStartCB>,
    pub stream_stop: Option<ReorderBufferStreamStopCB>,
    pub stream_abort: Option<ReorderBufferStreamAbortCB>,
    pub stream_prepare: Option<ReorderBufferStreamPrepareCB>,
    pub stream_commit: Option<ReorderBufferStreamCommitCB>,
    pub stream_change: Option<ReorderBufferStreamChangeCB>,
    pub stream_message: Option<ReorderBufferStreamMessageCB>,
    pub stream_truncate: Option<ReorderBufferStreamTruncateCB>,

    pub update_progress_txn: Option<ReorderBufferUpdateProgressTxnCB>,

    /// passed untouched to the callbacks (void *arg -> opaque).
    // TODO(ptr): caller-owned context; closure capture deferred.
    pub private_data: Option<Box<()>>,

    pub output_rewrites: bool,

    /* Memory contexts -> Rust ownership/arenas. */
    pub context: MemoryContext,
    pub change_context: MemoryContext,
    pub txn_context: MemoryContext,
    pub tup_context: MemoryContext,

    pub current_restart_decoding_lsn: XLogRecPtr,

    /// buffer for disk<->memory conversions.
    pub outbuf: Vec<u8>,

    /// memory accounting.
    pub size: usize,

    /// max-heap for sizes of all top-level and sub transactions (was pairingheap *).
    // Modeled as owned indices; comparator wiring deferred.
    pub txn_heap: Vec<TransactionId>,

    /* Spill-to-disk statistics. */
    pub spill_txns: i64,
    pub spill_count: i64,
    pub spill_bytes: i64,

    /* Streaming statistics. */
    pub stream_txns: i64,
    pub stream_count: i64,
    pub stream_bytes: i64,

    /* Totals. */
    pub total_txns: i64,
    pub total_bytes: i64,
}

/* --- txn_flags accessors (were rbtxn_* macros). --- */
impl ReorderBufferTXN {
    pub fn rbtxn_has_catalog_changes(&self) -> bool {
        self.txn_flags.contains(RbTxnFlags::HAS_CATALOG_CHANGES)
    }
    pub fn rbtxn_is_known_subxact(&self) -> bool {
        self.txn_flags.contains(RbTxnFlags::IS_SUBXACT)
    }
    pub fn rbtxn_is_serialized(&self) -> bool {
        self.txn_flags.contains(RbTxnFlags::IS_SERIALIZED)
    }
    pub fn rbtxn_is_serialized_clear(&self) -> bool {
        self.txn_flags.contains(RbTxnFlags::IS_SERIALIZED_CLEAR)
    }
    pub fn rbtxn_has_partial_change(&self) -> bool {
        self.txn_flags.contains(RbTxnFlags::HAS_PARTIAL_CHANGE)
    }
    pub fn rbtxn_has_streamable_change(&self) -> bool {
        self.txn_flags.contains(RbTxnFlags::HAS_STREAMABLE_CHANGE)
    }
    pub fn rbtxn_is_streamed(&self) -> bool {
        self.txn_flags.contains(RbTxnFlags::IS_STREAMED)
    }
    pub fn rbtxn_is_prepared(&self) -> bool {
        self.txn_flags.contains(RbTxnFlags::IS_PREPARED)
    }
    pub fn rbtxn_sent_prepare(&self) -> bool {
        self.txn_flags.contains(RbTxnFlags::SENT_PREPARE)
    }
    pub fn rbtxn_is_committed(&self) -> bool {
        self.txn_flags.contains(RbTxnFlags::IS_COMMITTED)
    }
    pub fn rbtxn_is_aborted(&self) -> bool {
        self.txn_flags.contains(RbTxnFlags::IS_ABORTED)
    }
    pub fn rbtxn_skip_prepared(&self) -> bool {
        self.txn_flags.contains(RbTxnFlags::SKIPPED_PREPARE)
    }
    pub fn rbtxn_distr_inval_overflowed(&self) -> bool {
        self.txn_flags.contains(RbTxnFlags::DISTR_INVAL_OVERFLOWED)
    }
    /// `toptxn == NULL`
    pub fn rbtxn_is_toptxn(&self) -> bool {
        self.toptxn.is_none()
    }
    /// `toptxn != NULL`
    pub fn rbtxn_is_subtxn(&self) -> bool {
        self.toptxn.is_some()
    }
}

/* --- Public API (stubs). --- */

pub fn ReorderBufferAllocate() -> Box<ReorderBuffer> {
    unimplemented!()
}
pub fn ReorderBufferFree(_rb: Box<ReorderBuffer>) {
    unimplemented!()
}

pub fn ReorderBufferAllocTupleBuf(_rb: &mut ReorderBuffer, _tuple_len: usize) -> Box<HeapTupleData> {
    unimplemented!()
}
pub fn ReorderBufferFreeTupleBuf(_tuple: Box<HeapTupleData>) {
    unimplemented!()
}

pub fn ReorderBufferAllocChange(_rb: &mut ReorderBuffer) -> Box<ReorderBufferChange> {
    unimplemented!()
}
pub fn ReorderBufferFreeChange(_rb: &mut ReorderBuffer, _change: Box<ReorderBufferChange>, _upd_mem: bool) {
    unimplemented!()
}

pub fn ReorderBufferAllocRelids(_rb: &mut ReorderBuffer, _nrelids: i32) -> Vec<Oid> {
    unimplemented!()
}
pub fn ReorderBufferFreeRelids(_rb: &mut ReorderBuffer, _relids: Vec<Oid>) {
    unimplemented!()
}

pub fn ReorderBufferQueueChange(
    _rb: &mut ReorderBuffer,
    _xid: TransactionId,
    _lsn: XLogRecPtr,
    _change: Box<ReorderBufferChange>,
    _toast_insert: bool,
) {
    unimplemented!()
}
pub fn ReorderBufferQueueMessage(
    _rb: &mut ReorderBuffer,
    _xid: TransactionId,
    _snap: OwnedSnapshot,
    _lsn: XLogRecPtr,
    _transactional: bool,
    _prefix: &str,
    _message: &[u8],
) {
    unimplemented!()
}
pub fn ReorderBufferCommit(
    _rb: &mut ReorderBuffer,
    _xid: TransactionId,
    _commit_lsn: XLogRecPtr,
    _end_lsn: XLogRecPtr,
    _commit_time: TimestampTz,
    _origin_id: RepOriginId,
    _origin_lsn: XLogRecPtr,
) {
    unimplemented!()
}
pub fn ReorderBufferFinishPrepared(
    _rb: &mut ReorderBuffer,
    _xid: TransactionId,
    _commit_lsn: XLogRecPtr,
    _end_lsn: XLogRecPtr,
    _two_phase_at: XLogRecPtr,
    _commit_time: TimestampTz,
    _origin_id: RepOriginId,
    _origin_lsn: XLogRecPtr,
    _gid: &str,
    _is_commit: bool,
) {
    unimplemented!()
}
pub fn ReorderBufferAssignChild(
    _rb: &mut ReorderBuffer,
    _xid: TransactionId,
    _subxid: TransactionId,
    _lsn: XLogRecPtr,
) {
    unimplemented!()
}
pub fn ReorderBufferCommitChild(
    _rb: &mut ReorderBuffer,
    _xid: TransactionId,
    _subxid: TransactionId,
    _commit_lsn: XLogRecPtr,
    _end_lsn: XLogRecPtr,
) {
    unimplemented!()
}
pub fn ReorderBufferAbort(
    _rb: &mut ReorderBuffer,
    _xid: TransactionId,
    _lsn: XLogRecPtr,
    _abort_time: TimestampTz,
) {
    unimplemented!()
}
pub fn ReorderBufferAbortOld(_rb: &mut ReorderBuffer, _oldest_running_xid: TransactionId) {
    unimplemented!()
}
pub fn ReorderBufferForget(_rb: &mut ReorderBuffer, _xid: TransactionId, _lsn: XLogRecPtr) {
    unimplemented!()
}
pub fn ReorderBufferInvalidate(_rb: &mut ReorderBuffer, _xid: TransactionId, _lsn: XLogRecPtr) {
    unimplemented!()
}

pub fn ReorderBufferSetBaseSnapshot(
    _rb: &mut ReorderBuffer,
    _xid: TransactionId,
    _lsn: XLogRecPtr,
    _snap: OwnedSnapshot,
) {
    unimplemented!()
}
pub fn ReorderBufferAddSnapshot(
    _rb: &mut ReorderBuffer,
    _xid: TransactionId,
    _lsn: XLogRecPtr,
    _snap: OwnedSnapshot,
) {
    unimplemented!()
}
pub fn ReorderBufferAddNewCommandId(
    _rb: &mut ReorderBuffer,
    _xid: TransactionId,
    _lsn: XLogRecPtr,
    _cid: CommandId,
) {
    unimplemented!()
}
pub fn ReorderBufferAddNewTupleCids(
    _rb: &mut ReorderBuffer,
    _xid: TransactionId,
    _lsn: XLogRecPtr,
    _locator: RelFileLocator,
    _tid: ItemPointerData,
    _cmin: CommandId,
    _cmax: CommandId,
    _combocid: CommandId,
) {
    unimplemented!()
}
pub fn ReorderBufferAddInvalidations(
    _rb: &mut ReorderBuffer,
    _xid: TransactionId,
    _lsn: XLogRecPtr,
    _msgs: &[SharedInvalidationMessage],
) {
    unimplemented!()
}
pub fn ReorderBufferAddDistributedInvalidations(
    _rb: &mut ReorderBuffer,
    _xid: TransactionId,
    _lsn: XLogRecPtr,
    _msgs: &[SharedInvalidationMessage],
) {
    unimplemented!()
}
pub fn ReorderBufferImmediateInvalidation(
    _rb: &mut ReorderBuffer,
    _invalidations: &[SharedInvalidationMessage],
) {
    unimplemented!()
}
pub fn ReorderBufferProcessXid(_rb: &mut ReorderBuffer, _xid: TransactionId, _lsn: XLogRecPtr) {
    unimplemented!()
}

pub fn ReorderBufferXidSetCatalogChanges(_rb: &mut ReorderBuffer, _xid: TransactionId, _lsn: XLogRecPtr) {
    unimplemented!()
}
pub fn ReorderBufferXidHasCatalogChanges(_rb: &mut ReorderBuffer, _xid: TransactionId) -> bool {
    unimplemented!()
}
pub fn ReorderBufferXidHasBaseSnapshot(_rb: &mut ReorderBuffer, _xid: TransactionId) -> bool {
    unimplemented!()
}

pub fn ReorderBufferRememberPrepareInfo(
    _rb: &mut ReorderBuffer,
    _xid: TransactionId,
    _prepare_lsn: XLogRecPtr,
    _end_lsn: XLogRecPtr,
    _prepare_time: TimestampTz,
    _origin_id: RepOriginId,
    _origin_lsn: XLogRecPtr,
) -> bool {
    unimplemented!()
}
pub fn ReorderBufferSkipPrepare(_rb: &mut ReorderBuffer, _xid: TransactionId) {
    unimplemented!()
}
pub fn ReorderBufferPrepare(_rb: &mut ReorderBuffer, _xid: TransactionId, _gid: &str) {
    unimplemented!()
}
/// Returns the oldest TXN, or None if the buffer is empty.
pub fn ReorderBufferGetOldestTXN(_rb: &mut ReorderBuffer) -> Option<&mut ReorderBufferTXN> {
    unimplemented!()
}
pub fn ReorderBufferGetOldestXmin(_rb: &mut ReorderBuffer) -> TransactionId {
    unimplemented!()
}
pub fn ReorderBufferGetCatalogChangesXacts(_rb: &mut ReorderBuffer) -> Vec<TransactionId> {
    unimplemented!()
}

pub fn ReorderBufferSetRestartPoint(_rb: &mut ReorderBuffer, _ptr: XLogRecPtr) {
    unimplemented!()
}

/// C out-param `SharedInvalidationMessage **msgs` + returned count -> Vec.
pub fn ReorderBufferGetInvalidations(
    _rb: &mut ReorderBuffer,
    _xid: TransactionId,
) -> Vec<SharedInvalidationMessage> {
    unimplemented!()
}

pub fn StartupReorderBuffer() {
    unimplemented!()
}
