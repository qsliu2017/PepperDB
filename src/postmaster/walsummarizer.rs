//! Translated from PostgreSQL src/include/postmaster/walsummarizer.h
//! Background WAL summarization process.

use crate::access::xlogdefs::{TimeLineID, XLogRecPtr};

// GUC variables. TODO(global)
pub static mut summarize_wal: bool = false;
pub static mut wal_summary_keep_time: i32 = 0;

// Shared-memory sizing/init: shmem -> Arc-shared heap state in single process.
pub fn WalSummarizerShmemSize() -> usize {
    unimplemented!()
}
pub fn WalSummarizerShmemInit() {
    unimplemented!()
}

/// C: `pg_noreturn ... WalSummarizerMain(const void*, size_t)`.
pub fn WalSummarizerMain(startup_data: &[u8]) -> ! {
    unimplemented!()
}

/// C: out-params (summarized_tli, summarized_lsn, pending_lsn, summarizer_pid).
pub struct WalSummarizerState {
    pub summarized_tli: TimeLineID,
    pub summarized_lsn: XLogRecPtr,
    pub pending_lsn: XLogRecPtr,
    pub summarizer_pid: i32,
}

pub fn GetWalSummarizerState() -> WalSummarizerState {
    unimplemented!()
}

/// C: `GetOldestUnsummarizedLSN(TimeLineID *tli, bool *lsn_is_exact)` -> the LSN
/// plus the two out-params returned alongside.
pub fn GetOldestUnsummarizedLSN() -> (XLogRecPtr, TimeLineID, bool) {
    unimplemented!()
}

pub fn WakeupWalSummarizer() {
    unimplemented!()
}
pub fn WaitForWalSummarization(lsn: XLogRecPtr) {
    unimplemented!()
}
