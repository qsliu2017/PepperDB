//! Basebackup sink implementing progress tracking, including but not limited to
//! command progress reporting.
//!
//! This should be used even if the PROGRESS option to the replication command
//! BASE_BACKUP is not specified. Without that option, we won't have tallied up
//! the size of the files that are going to need to be backed up, but we can
//! still report to the command progress reporting facility how much data we've
//! processed.
//!
//! Moreover, we also use this as a convenient place to update certain fields of
//! the bbsink_state. That work is accurately described as keeping track of our
//! progress, but it's not just for introspection. We need those fields to be
//! updated properly in order for base backups to work.
//!
//! This particular basebackup sink requires extra callbacks that most base
//! backup sinks don't. Rather than cramming those into the interface, we just
//! have a few extra functions here that basebackup.c can call.
//!
//! Source: postgres/src/backend/backup/basebackup_progress.c
//!
//! #include mapping:
//!   "postgres.h"                -> use crate::prelude::*
//!   "backup/basebackup_sink.h"  -> crate::backup::basebackup_sink (PORTED)
//!   "commands/progress.h"       -> PROGRESS_BASEBACKUP_* index/phase consts (STUB:
//!                                  local consts with the real C values, see below)
//!   "pgstat.h"                  -> pgstat_progress_* command progress reporting
//!                                  (STUB: no-ops, see below)

use crate::prelude::*;

use crate::backup::basebackup_sink::{
    bbsink, bbsink_forward_begin_archive, bbsink_forward_begin_backup,
    bbsink_forward_begin_manifest, bbsink_forward_cleanup, bbsink_forward_end_archive,
    bbsink_forward_end_backup, bbsink_forward_end_manifest, bbsink_forward_manifest_contents,
    bbsink_forward_archive_contents, bbsink_ops, bbsink_state,
};
use crate::nodes::pg_list::list_length;

// ---------------------------------------------------------------------------
// Stubs for as-yet-unported dependencies.
// ---------------------------------------------------------------------------

// commands/progress.h: progress-report parameter indices for pg_basebackup.
// STUB (header not yet ported): local consts carrying the exact C values.
// TODO: port commands/progress.h.
const PROGRESS_BASEBACKUP_PHASE: c_int = 0;
const PROGRESS_BASEBACKUP_BACKUP_TOTAL: c_int = 1;
const PROGRESS_BASEBACKUP_BACKUP_STREAMED: c_int = 2;
const PROGRESS_BASEBACKUP_TBLSPC_TOTAL: c_int = 3;
const PROGRESS_BASEBACKUP_TBLSPC_STREAMED: c_int = 4;

// commands/progress.h: phase values advertised via PROGRESS_BASEBACKUP_PHASE.
// STUB (header not yet ported): exact C values.
const PROGRESS_BASEBACKUP_PHASE_WAIT_CHECKPOINT: int64 = 1;
const PROGRESS_BASEBACKUP_PHASE_ESTIMATE_BACKUP_SIZE: int64 = 2;
const PROGRESS_BASEBACKUP_PHASE_STREAM_BACKUP: int64 = 3;
const PROGRESS_BASEBACKUP_PHASE_WAIT_WAL_ARCHIVE: int64 = 4;
const PROGRESS_BASEBACKUP_PHASE_TRANSFER_WAL: int64 = 5;

// pgstat.h: PROGRESS_COMMAND_BASEBACKUP command id (enum ProgressCommandType).
// STUB: arbitrary placeholder; only passed to the no-op pgstat shims below.
// TODO: port the pgstat ProgressCommandType enum.
const PROGRESS_COMMAND_BASEBACKUP: c_int = 4;

// utils/adt/oid.h InvalidOid: passed through to the no-op pgstat shim. The crate
// exposes Oid via the prelude; mirror the canonical InvalidOid value.
const InvalidOid: Oid = 0;

// pgstat.h: pgstat_progress_start_command begins command progress reporting for
// the current backend. STUB: no-op (the cumulative stats system is unported).
// TODO: port utils/activity/backend_progress.c pgstat_progress_start_command.
unsafe fn pgstat_progress_start_command(_cmdtype: c_int, _relid: Oid) {}

// pgstat.h: pgstat_progress_update_param updates a single progress parameter.
// STUB: no-op (cumulative stats system unported).
// TODO: port utils/activity/backend_progress.c pgstat_progress_update_param.
unsafe fn pgstat_progress_update_param(_index: c_int, _val: int64) {}

// pgstat.h: pgstat_progress_update_multi_param updates several progress
// parameters at once. STUB: no-op (cumulative stats system unported).
// TODO: port utils/activity/backend_progress.c pgstat_progress_update_multi_param.
unsafe fn pgstat_progress_update_multi_param(_nparam: c_int, _index: *const c_int, _val: *const int64) {}

// pgstat.h: pgstat_progress_end_command finishes command progress reporting.
// STUB: no-op (cumulative stats system unported).
// TODO: port utils/activity/backend_progress.c pgstat_progress_end_command.
unsafe fn pgstat_progress_end_command() {}

// ---------------------------------------------------------------------------
// bbsink_progress: a progress-tracking sink decorator. It carries no extra
// fields beyond the common bbsink, so the C source allocates a bare bbsink. We
// mirror that exactly: there is no bbsink_progress struct, and the *mut bbsink
// is its own "downcast".
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Ops table: begin_backup/archive_contents/end_archive are overridden by this
// file; everything else forwards to the successor sink.
// ---------------------------------------------------------------------------
static bbsink_progress_ops: bbsink_ops = bbsink_ops {
    begin_backup: Some(bbsink_progress_begin_backup),
    begin_archive: Some(bbsink_forward_begin_archive),
    archive_contents: Some(bbsink_progress_archive_contents),
    end_archive: Some(bbsink_progress_end_archive),
    begin_manifest: Some(bbsink_forward_begin_manifest),
    manifest_contents: Some(bbsink_forward_manifest_contents),
    end_manifest: Some(bbsink_forward_end_manifest),
    end_backup: Some(bbsink_forward_end_backup),
    cleanup: Some(bbsink_forward_cleanup),
};

/// Create a new basebackup sink that performs progress tracking functions and
/// forwards data to a successor sink.
pub unsafe fn bbsink_progress_new(next: *mut bbsink, _estimate_backup_size: bool) -> *mut bbsink {
    Assert!(!next.is_null());

    let sink = palloc0(core::mem::size_of::<bbsink>()) as *mut bbsink;
    (*sink).bbs_ops = &bbsink_progress_ops;
    (*sink).bbs_next = next;

    // Report that a base backup is in progress, and set the total size of the
    // backup to -1, which will get translated to NULL. If we're estimating the
    // backup size, we'll insert the real estimate when we have it.
    pgstat_progress_start_command(PROGRESS_COMMAND_BASEBACKUP, InvalidOid);
    pgstat_progress_update_param(PROGRESS_BASEBACKUP_BACKUP_TOTAL, -1);

    sink
}

/// Progress reporting at start of backup.
unsafe fn bbsink_progress_begin_backup(sink: *mut bbsink) {
    let index: [c_int; 3] = [
        PROGRESS_BASEBACKUP_PHASE,
        PROGRESS_BASEBACKUP_BACKUP_TOTAL,
        PROGRESS_BASEBACKUP_TBLSPC_TOTAL,
    ];
    let mut val: [int64; 3] = [0; 3];

    // Report that we are now streaming database files as a base backup. Also
    // advertise the number of tablespaces, and, if known, the estimated total
    // backup size.
    val[0] = PROGRESS_BASEBACKUP_PHASE_STREAM_BACKUP;
    if (*(*sink).bbs_state).bytes_total_is_valid {
        val[1] = (*(*sink).bbs_state).bytes_total as int64;
    } else {
        val[1] = -1;
    }
    val[2] = list_length((*(*sink).bbs_state).tablespaces) as int64;
    pgstat_progress_update_multi_param(3, index.as_ptr(), val.as_ptr());

    // Delegate to next sink.
    bbsink_forward_begin_backup(sink);
}

/// End-of archive progress reporting.
unsafe fn bbsink_progress_end_archive(sink: *mut bbsink) {
    // We expect one archive per tablespace, so reaching the end of an archive
    // also means reaching the end of a tablespace. (Some day we might have a
    // reason to decouple these concepts.)
    //
    // If WAL is included in the backup, we'll mark the last tablespace complete
    // before the last archive is complete, so we need a guard here to ensure
    // that the number of tablespaces streamed doesn't exceed the total.
    if (*(*sink).bbs_state).tablespace_num < list_length((*(*sink).bbs_state).tablespaces) {
        pgstat_progress_update_param(
            PROGRESS_BASEBACKUP_TBLSPC_STREAMED,
            ((*(*sink).bbs_state).tablespace_num + 1) as int64,
        );
    }

    // Delegate to next sink.
    bbsink_forward_end_archive(sink);

    // This is a convenient place to update the bbsink_state's notion of which is
    // the current tablespace. Note that the bbsink_state object is shared across
    // all bbsink objects involved, but we're the outermost one and this is the
    // very last thing we do.
    (*(*sink).bbs_state).tablespace_num += 1;
}

/// Handle progress tracking for new archive contents.
///
/// Increment the counter for the amount of data already streamed by the given
/// number of bytes, and update the progress report for
/// pg_stat_progress_basebackup.
unsafe fn bbsink_progress_archive_contents(sink: *mut bbsink, len: Size) {
    let state: *mut bbsink_state = (*sink).bbs_state;
    let index: [c_int; 2] = [
        PROGRESS_BASEBACKUP_BACKUP_STREAMED,
        PROGRESS_BASEBACKUP_BACKUP_TOTAL,
    ];
    let mut val: [int64; 2] = [0; 2];
    let mut nparam: usize = 0;

    // First update bbsink_state with # of bytes done.
    (*state).bytes_done += len as uint64;

    // Now forward to next sink.
    bbsink_forward_archive_contents(sink, len);

    // Prepare to set # of bytes done for command progress reporting.
    val[nparam] = (*state).bytes_done as int64;
    nparam += 1;

    // We may also want to update # of total bytes, to avoid overflowing past
    // 100% or the full size. This may make the total size number change as we
    // approach the end of the backup (the estimate will always be wrong if WAL
    // is included), but that's better than having the done column be bigger than
    // the total.
    if (*state).bytes_total_is_valid && (*state).bytes_done > (*state).bytes_total {
        val[nparam] = (*state).bytes_done as int64;
        nparam += 1;
    }

    pgstat_progress_update_multi_param(nparam as c_int, index.as_ptr(), val.as_ptr());
}

/// Advertise that we are waiting for the start-of-backup checkpoint.
pub unsafe fn basebackup_progress_wait_checkpoint() {
    pgstat_progress_update_param(
        PROGRESS_BASEBACKUP_PHASE,
        PROGRESS_BASEBACKUP_PHASE_WAIT_CHECKPOINT,
    );
}

/// Advertise that we are estimating the backup size.
pub unsafe fn basebackup_progress_estimate_backup_size() {
    pgstat_progress_update_param(
        PROGRESS_BASEBACKUP_PHASE,
        PROGRESS_BASEBACKUP_PHASE_ESTIMATE_BACKUP_SIZE,
    );
}

/// Advertise that we are waiting for WAL archiving at end-of-backup.
pub unsafe fn basebackup_progress_wait_wal_archive(state: *mut bbsink_state) {
    let index: [c_int; 2] = [PROGRESS_BASEBACKUP_PHASE, PROGRESS_BASEBACKUP_TBLSPC_STREAMED];
    let mut val: [int64; 2] = [0; 2];

    // We report having finished all tablespaces at this point, even if the
    // archive for the main tablespace is still open, because what's going to be
    // added is WAL files, not files that are really from the main tablespace.
    val[0] = PROGRESS_BASEBACKUP_PHASE_WAIT_WAL_ARCHIVE;
    val[1] = list_length((*state).tablespaces) as int64;
    pgstat_progress_update_multi_param(2, index.as_ptr(), val.as_ptr());
}

/// Advertise that we are transferring WAL files into the final archive.
pub unsafe fn basebackup_progress_transfer_wal() {
    pgstat_progress_update_param(
        PROGRESS_BASEBACKUP_PHASE,
        PROGRESS_BASEBACKUP_PHASE_TRANSFER_WAL,
    );
}

/// Advertise that we are no longer performing a backup.
pub unsafe fn basebackup_progress_done() {
    pgstat_progress_end_command();
}

#[cfg(test)]
mod tests {
    use super::*;

    // bbsink_progress_new must wire the progress ops vtable and the successor
    // (bbs_next). The pgstat shims are no-ops, so the constructor only does
    // allocation + pointer wiring; the successor is never dereferenced here.
    #[test]
    fn new_wires_ops_and_next() {
        unsafe {
            let next = palloc0(core::mem::size_of::<bbsink>()) as *mut bbsink;
            let sink = bbsink_progress_new(next, false);

            assert_eq!((*sink).bbs_next, next);
            assert!(core::ptr::eq((*sink).bbs_ops, &bbsink_progress_ops));

            pfree(sink as *mut c_void);
            pfree(next as *mut c_void);
        }
    }

    // archive_contents accounting: each call must add `len` to the running
    // bytes_done total in the shared bbsink_state. We feed a known length and a
    // terminal successor whose forwarding requires a shared buffer. With
    // bytes_total_is_valid = false, the update path stays purely additive.
    static mut TERMINAL_HITS: c_int = 0;

    unsafe fn terminal_archive_contents(_sink: *mut bbsink, _len: Size) {
        TERMINAL_HITS += 1;
    }

    static TERMINAL_OPS: bbsink_ops = bbsink_ops {
        begin_backup: None,
        begin_archive: None,
        archive_contents: Some(terminal_archive_contents),
        end_archive: None,
        begin_manifest: None,
        manifest_contents: None,
        end_manifest: None,
        end_backup: None,
        cleanup: None,
    };

    #[test]
    fn archive_contents_updates_streamed_total() {
        unsafe {
            use crate::pg_config::BLCKSZ;

            TERMINAL_HITS = 0;

            // Shared buffer so bbsink_forward_archive_contents' Asserts pass.
            let buffer_length: Size = BLCKSZ;
            let buffer = palloc0(buffer_length) as *mut c_char;

            // Shared state, owned by the front sink.
            let state = palloc0(core::mem::size_of::<bbsink_state>()) as *mut bbsink_state;
            (*state).tablespaces = null_mut();
            (*state).tablespace_num = 0;
            (*state).bytes_done = 0;
            (*state).bytes_total = 0;
            (*state).bytes_total_is_valid = false;

            // Terminal successor sink.
            let next = palloc0(core::mem::size_of::<bbsink>()) as *mut bbsink;
            (*next).bbs_ops = &TERMINAL_OPS;
            (*next).bbs_buffer = buffer;
            (*next).bbs_buffer_length = buffer_length;
            (*next).bbs_next = null_mut();
            (*next).bbs_state = state;

            // Front progress sink, sharing buffer + state with the successor.
            let front = bbsink_progress_new(next, false);
            (*front).bbs_buffer = buffer;
            (*front).bbs_buffer_length = buffer_length;
            (*front).bbs_state = state;

            // Feed two known lengths; bytes_done must be their running sum.
            bbsink_progress_archive_contents(front, 4096);
            assert_eq!((*state).bytes_done, 4096);

            bbsink_progress_archive_contents(front, 1000);
            assert_eq!((*state).bytes_done, 5096);

            // Forwarding actually reached the terminal sink both times.
            assert_eq!(TERMINAL_HITS, 2);

            pfree(front as *mut c_void);
            pfree(next as *mut c_void);
            pfree(state as *mut c_void);
            pfree(buffer as *mut c_void);
        }
    }
}
