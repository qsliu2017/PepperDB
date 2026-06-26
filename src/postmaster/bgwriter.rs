//! Translated from PostgreSQL src/include/postmaster/bgwriter.h
//!
//! This header declares both the background-writer and the CHECKPOINTER public
//! API. The checkpointer bodies live in `crate::backend::postmaster::checkpointer`
//! (step 17a); this header rewires the public symbols to that module. The
//! background-writer bodies (BackgroundWriterMain) land in step 17b.

// GUC options. PG declares these in bgwriter.h; the GUC copies live as
// process-global atomics in the backend modules with accessor functions (no
// `static mut`).

/// PG `BgWriterDelay` GUC accessor (backed by bgwriter.rs, step 17b).
pub use crate::backend::postmaster::bgwriter::bg_writer_delay as BgWriterDelay;

/// PG `BackgroundWriterMain` - the long-lived bgwriter aux task (step 17b).
pub use crate::backend::postmaster::bgwriter::background_writer_main as BackgroundWriterMain;

/// PG `CheckPointTimeout` (accessor; backed by checkpointer.rs).
pub use crate::backend::postmaster::checkpointer::check_point_timeout as CheckPointTimeout;

// --- Checkpointer public API (checkpointer.c bodies, step 17a) ---

/// PG `RequestCheckpoint` (async under the single-process model).
pub use crate::backend::postmaster::checkpointer::request_checkpoint as RequestCheckpoint;
/// PG `CheckpointWriteDelay` (async; throttling body lands with BufferSync).
pub use crate::backend::postmaster::checkpointer::checkpoint_write_delay as CheckpointWriteDelay;
/// PG `AbsorbSyncRequests` (no-op: single-process drains the shared queue).
pub use crate::backend::postmaster::checkpointer::absorb_sync_requests as AbsorbSyncRequests;
/// PG `FirstCallSinceLastCheckpoint`.
pub use crate::backend::postmaster::checkpointer::first_call_since_last_checkpoint as FirstCallSinceLastCheckpoint;

// ForwardSyncRequest / CompactCheckpointerRequestQueue / CheckpointerShmemSize /
// CheckpointerShmemInit are deleted by redesign (single-process uses the shared
// SyncRequests queue + the Arc<CheckpointerShmem> on SharedState).
