//! File synchronization management. Translated from backend/storage/sync/sync.c.
//!
//! Tracks the relation segments that have been written since the last
//! checkpoint so they can be fsynced to disk before the next checkpoint
//! completes. Pending fsync operations are held in a hash table, which also
//! serves to merge duplicate requests; no-longer-needed files awaiting deletion
//! are held in a list, since duplicates are not expected there. A pair of cycle
//! counters distinguishes requests entered before a checkpoint began from those
//! entered during it, so that fsyncs are not skipped and files are not deleted
//! too soon. These mechanisms apply only to non-temp relations. Each request
//! carries a `FileTag` that names the owning storage manager; the sync, unlink,
//! and match operations are dispatched to that manager (currently only md, the
//! magnetic-disk manager).
//!
//! In PostgreSQL the pending-ops table is private to whichever process keeps it
//! -- a standalone backend or the checkpointer auxiliary process -- and regular
//! backends forward their requests to the checkpointer over a shared-memory
//! queue. PepperDB runs as a single process, so there is one shared
//! [`SyncRequests`] structure (an [`Arc`] field on [`SharedState`]) guarded by a
//! `parking_lot::Mutex`; every task enqueues into it directly and the
//! checkpointer task drains it. Each entry-collecting routine takes the tags to
//! act on under the lock, releases it, and only then performs the fsync or
//! unlink, so the lock is never held across an `.await`. An fsync failure raises
//! a PANIC (or an error if data-sync retry is enabled), preserving PostgreSQL's
//! guarantee that a checkpoint never falsely reports durability. The SLRU sync
//! handlers (clog, commit_ts, multixact) are not yet wired in.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::backend::storage::smgr::md;
use crate::shared_state::SharedState;
use crate::storage::sync::{FileTag, SyncRequestHandler, SyncRequestType};

/// Cycle counter (sync.c `CycleCtr`); distinguishes requests entered before vs
/// during a checkpoint.
type CycleCtr = u16;

/// A pending fsync request (sync.c `PendingFsyncEntry`).
struct PendingFsyncEntry {
    cycle_ctr: CycleCtr,
    canceled: bool,
}

/// A pending unlink request (sync.c `PendingUnlinkEntry`).
struct PendingUnlinkEntry {
    tag: FileTag,
    cycle_ctr: CycleCtr,
    canceled: bool,
}

/// The mutable interior of [`SyncRequests`].
struct SyncRequestsInner {
    /// Pending fsyncs, keyed by file tag (merges duplicate requests).
    pending_ops: HashMap<FileTagKey, PendingFsyncEntry>,
    /// Pending unlinks (a list; duplicates are not expected).
    pending_unlinks: Vec<PendingUnlinkEntry>,
    sync_cycle_ctr: CycleCtr,
    checkpoint_cycle_ctr: CycleCtr,
    /// True while a ProcessSyncRequests run is in flight; if still set on entry a
    /// prior run failed, so stale cycle_ctr values must be re-armed (sync.c).
    sync_in_progress: bool,
}

/// A hashable key for a [`FileTag`] (the header `FileTag` is the identity).
#[derive(Clone, PartialEq, Eq, Hash)]
struct FileTagKey {
    handler: i16,
    forknum: i16,
    spc: u32,
    db: u32,
    rel: u32,
    segno: u64,
}

impl FileTagKey {
    fn of(tag: &FileTag) -> Self {
        Self {
            handler: tag.handler,
            forknum: tag.forknum,
            spc: tag.rlocator.spcOid.0,
            db: tag.rlocator.dbOid.0,
            rel: tag.rlocator.relNumber.0,
            segno: tag.segno,
        }
    }
}

/// The shared pending-ops queue (replaces sync.c's per-process `pendingOps` /
/// `pendingUnlinks`). Lives as an `Arc` field on [`SharedState`]; drained by the
/// checkpointer (step 17).
pub struct SyncRequests {
    inner: Mutex<SyncRequestsInner>,
}

impl SyncRequests {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(SyncRequestsInner {
                pending_ops: HashMap::new(),
                pending_unlinks: Vec::new(),
                sync_cycle_ctr: 0,
                checkpoint_cycle_ctr: 0,
                sync_in_progress: false,
            }),
        }
    }

    /// Number of (non-canceled) pending fsync requests -- for tests.
    pub fn pending_op_count(&self) -> usize {
        self.inner
            .lock()
            .pending_ops
            .values()
            .filter(|e| !e.canceled)
            .count()
    }

    /// True if a sync request for `tag` is queued (and not canceled) -- tests.
    pub fn has_pending_sync(&self, tag: &FileTag) -> bool {
        let g = self.inner.lock();
        g.pending_ops
            .get(&FileTagKey::of(tag))
            .is_some_and(|e| !e.canceled)
    }

    pub fn pending_unlink_count(&self) -> usize {
        self.inner
            .lock()
            .pending_unlinks
            .iter()
            .filter(|e| !e.canceled)
            .count()
    }
}

impl Default for SyncRequests {
    fn default() -> Self {
        Self::new()
    }
}

/// InitSync() -- nothing to do; the shared queue is constructed in
/// `SharedState::new`. Kept for call-site parity.
pub fn InitSync() {
    // The pending-ops queue is an Arc field on SharedState (single-process), so
    // there is no per-process table to create here.
}

/// SyncPreCheckpoint() -- advance the unlink cycle counter so unlinks arriving
/// after this point wait for the next checkpoint.
pub fn SyncPreCheckpoint(shared: &Arc<SharedState>) {
    let mut g = shared.sync_requests().inner.lock();
    g.checkpoint_cycle_ctr = g.checkpoint_cycle_ctr.wrapping_add(1);
}

/// SyncPostCheckpoint() -- unlink files that are now safe to remove. Collects
/// the tags under the lock, drops it, then unlinks (no lock across `.await`).
pub async fn SyncPostCheckpoint(shared: &Arc<SharedState>) {
    let to_unlink: Vec<FileTag> = {
        let mut g = shared.sync_requests().inner.lock();
        let checkpoint_cycle = g.checkpoint_cycle_ctr;
        let mut tags = Vec::new();
        // New entries are appended; stop at the first entry from this cycle.
        let mut keep_from = g.pending_unlinks.len();
        for (i, e) in g.pending_unlinks.iter().enumerate() {
            if e.cycle_ctr == checkpoint_cycle {
                keep_from = i;
                break;
            }
            keep_from = i + 1;
            if !e.canceled {
                tags.push(e.tag);
            }
        }
        g.pending_unlinks.drain(..keep_from);
        tags
    };

    for tag in &to_unlink {
        if let Err(e) = unlink_filetag(shared, tag).await
            && e.kind() != std::io::ErrorKind::NotFound {
                crate::elog!(
                    crate::utils::elog::WARNING,
                    format!("could not remove file: {e}")
                );
            }
    }
}

/// ProcessSyncRequests() -- fsync all pending segments (the checkpointer's sync
/// phase). Collects the tags under the lock, drops it, fsyncs, then removes the
/// processed entries. No lock is held across the fsync `.await`.
///
/// Called by the checkpointer task during its sync phase (checkpointer.rs).
pub async fn ProcessSyncRequests(shared: &Arc<SharedState>) {
    let sr = shared.sync_requests();

    // Snapshot the entries to process and arm the cycle counter under the lock.
    // We process every entry that is NOT new (cycle_ctr != the post-increment
    // value), matching sync.c, and remove all of them -- including canceled ones.
    let to_process: Vec<(FileTag, bool)> = {
        let mut g = sr.inner.lock();
        // If a prior run did not complete, forcibly re-arm stale cycle_ctr values
        // so left-behind entries are guaranteed to be picked up (forestalls u16
        // wraparound silently skipping them).
        if g.sync_in_progress {
            let cur = g.sync_cycle_ctr;
            for e in g.pending_ops.values_mut() {
                e.cycle_ctr = cur;
            }
        }
        g.sync_cycle_ctr = g.sync_cycle_ctr.wrapping_add(1);
        g.sync_in_progress = true;
        let new_cycle = g.sync_cycle_ctr;
        g.pending_ops
            .iter()
            .filter(|(_, e)| e.cycle_ctr != new_cycle)
            .map(|(k, e)| (key_to_tag(k), e.canceled))
            .collect()
    };

    let enable_fsync = unsafe { crate::miscadmin::enableFsync };

    for (tag, canceled) in to_process {
        // A canceled entry skips the fsync but is still removed (sync.c).
        // fsync off: don't even open the file, but still remove the entry.
        if !canceled && enable_fsync {
            match sync_filetag(shared, &tag).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    // Relation dropped/truncated since the request: allow ENOENT.
                }
                Err(e) => {
                    // data_sync_elevel: PANIC unless data_sync_retry is set, so a
                    // checkpoint can never falsely report durability.
                    let elevel = if unsafe { crate::storage::fd::data_sync_retry } {
                        crate::utils::elog::ERROR
                    } else {
                        crate::utils::elog::PANIC
                    };
                    crate::elog!(elevel, format!("could not fsync file: {e}"));
                }
            }
        }
        sr.inner.lock().pending_ops.remove(&FileTagKey::of(&tag));
    }

    sr.inner.lock().sync_in_progress = false;
}

fn key_to_tag(k: &FileTagKey) -> FileTag {
    FileTag {
        handler: k.handler,
        forknum: k.forknum,
        rlocator: crate::storage::relfilelocator::RelFileLocator {
            spcOid: crate::postgres_ext::Oid(k.spc),
            dbOid: crate::postgres_ext::Oid(k.db),
            relNumber: crate::postgres_ext::Oid(k.rel),
        },
        segno: k.segno,
    }
}

/// Dispatch a sync to the owning module (sync.c `syncsw[...].sync_syncfiletag`).
/// Only md exists.
async fn sync_filetag(shared: &Arc<SharedState>, tag: &FileTag) -> std::io::Result<String> {
    match tag.handler() {
        SyncRequestHandler::Md => md::mdsyncfiletag(shared, tag).await,
        // TODO(slru): clog/commit_ts/multixact sync handlers.
        _ => Ok(String::new()),
    }
}

/// Dispatch an unlink to the owning module (sync.c `sync_unlinkfiletag`).
async fn unlink_filetag(shared: &Arc<SharedState>, tag: &FileTag) -> std::io::Result<String> {
    match tag.handler() {
        SyncRequestHandler::Md => md::mdunlinkfiletag(shared, tag).await,
        _ => Ok(String::new()),
    }
}

/// Dispatch a filter predicate (sync.c `sync_filetagmatches`).
fn filetag_matches(tag: &FileTag, candidate: &FileTag) -> bool {
    match tag.handler() {
        SyncRequestHandler::Md => md::mdfiletagmatches(tag, candidate),
        _ => false,
    }
}

/// RememberSyncRequest() -- enter/cancel a request in the shared queue (sync.c).
pub fn RememberSyncRequest(shared: &Arc<SharedState>, ftag: &FileTag, req_type: SyncRequestType) {
    shared.sync_requests().register_tag(ftag, req_type);
}

impl SyncRequests {
    /// Enter/cancel a request directly on the queue (used by callers that hold
    /// an `Arc<SyncRequests>` rather than the whole `SharedState`, e.g. SLRU).
    pub fn register_tag(&self, ftag: &FileTag, req_type: SyncRequestType) {
        let mut g = self.inner.lock();
        register_tag_locked(&mut g, ftag, req_type);
    }
}

fn register_tag_locked(g: &mut SyncRequestsInner, ftag: &FileTag, req_type: SyncRequestType) {
    match req_type {
        SyncRequestType::SyncForgetRequest => {
            if let Some(e) = g.pending_ops.get_mut(&FileTagKey::of(ftag)) {
                e.canceled = true;
            }
        }
        SyncRequestType::SyncFilterRequest => {
            // Cancel matching fsync + unlink requests (same database, per md).
            let matching: Vec<FileTagKey> = g
                .pending_ops
                .keys()
                .filter(|k| k.handler == ftag.handler && filetag_matches(ftag, &key_to_tag(k)))
                .cloned()
                .collect();
            for k in matching {
                if let Some(e) = g.pending_ops.get_mut(&k) {
                    e.canceled = true;
                }
            }
            for pue in &mut g.pending_unlinks {
                if pue.tag.handler == ftag.handler && filetag_matches(ftag, &pue.tag) {
                    pue.canceled = true;
                }
            }
        }
        SyncRequestType::SyncUnlinkRequest => {
            let cycle = g.checkpoint_cycle_ctr;
            g.pending_unlinks.push(PendingUnlinkEntry {
                tag: *ftag,
                cycle_ctr: cycle,
                canceled: false,
            });
        }
        SyncRequestType::SyncRequest => {
            let cycle = g.sync_cycle_ctr;
            let entry = g
                .pending_ops
                .entry(FileTagKey::of(ftag))
                .or_insert(PendingFsyncEntry {
                    cycle_ctr: cycle,
                    canceled: false,
                });
            if entry.canceled {
                entry.cycle_ctr = cycle;
                entry.canceled = false;
            }
            // NB: don't change cycle_ctr of an existing live entry (it must be
            // the oldest request's cycle).
        }
    }
}

/// RegisterSyncRequest() -- enqueue a sync request. In the single-process model
/// this always succeeds (the queue is an in-memory Arc structure), so the
/// `retry_on_error` loop in sync.c collapses to a single insert.
pub fn RegisterSyncRequest(
    shared: &Arc<SharedState>,
    ftag: &FileTag,
    req_type: SyncRequestType,
    _retry_on_error: bool,
) -> bool {
    RememberSyncRequest(shared, ftag, req_type);
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::relpath::ForkNumber;
    use crate::postgres_ext::Oid;
    use crate::storage::relfilelocator::RelFileLocator;

    fn tag(rel: u32, segno: u64) -> FileTag {
        FileTag {
            handler: SyncRequestHandler::Md as i16,
            forknum: ForkNumber::MAIN_FORKNUM as i16,
            rlocator: RelFileLocator {
                spcOid: Oid(1663),
                dbOid: Oid(5),
                relNumber: Oid(rel),
            },
            segno,
        }
    }

    #[test]
    fn register_and_forget() {
        let s = SharedState::new(crate::shared_state::SharedStateConfig::default());
        let t = tag(100, 0);
        assert!(!s.sync_requests().has_pending_sync(&t));
        RegisterSyncRequest(&s, &t, SyncRequestType::SyncRequest, false);
        assert!(s.sync_requests().has_pending_sync(&t));
        assert_eq!(s.sync_requests().pending_op_count(), 1);

        // Duplicate request merges.
        RegisterSyncRequest(&s, &t, SyncRequestType::SyncRequest, false);
        assert_eq!(s.sync_requests().pending_op_count(), 1);

        // Forget cancels it.
        RegisterSyncRequest(&s, &t, SyncRequestType::SyncForgetRequest, true);
        assert!(!s.sync_requests().has_pending_sync(&t));
    }

    #[test]
    fn filter_cancels_by_database() {
        let s = SharedState::new(crate::shared_state::SharedStateConfig::default());
        RegisterSyncRequest(&s, &tag(1, 0), SyncRequestType::SyncRequest, false);
        RegisterSyncRequest(&s, &tag(2, 0), SyncRequestType::SyncRequest, false);
        assert_eq!(s.sync_requests().pending_op_count(), 2);

        // Filter on the same db cancels both.
        let filt = FileTag {
            handler: SyncRequestHandler::Md as i16,
            forknum: ForkNumber::InvalidForkNumber as i16,
            rlocator: RelFileLocator {
                spcOid: Oid(0),
                dbOid: Oid(5),
                relNumber: Oid(0),
            },
            segno: 0,
        };
        RegisterSyncRequest(&s, &filt, SyncRequestType::SyncFilterRequest, true);
        assert_eq!(s.sync_requests().pending_op_count(), 0);
    }
}
