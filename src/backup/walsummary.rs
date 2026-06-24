//! Translated from PostgreSQL src/include/backup/walsummary.h
//! WAL summary management.

use crate::access::xlogdefs::{TimeLineID, XLogRecPtr};
use crate::storage::fd::File;

pub struct WalSummaryIO {
    pub file: File,
    pub filepos: i64, // off_t
}

pub struct WalSummaryFile {
    pub start_lsn: XLogRecPtr,
    pub end_lsn: XLogRecPtr,
    pub tli: TimeLineID,
}

pub fn get_wal_summaries(
    _tli: TimeLineID,
    _start_lsn: XLogRecPtr,
    _end_lsn: XLogRecPtr,
) -> Vec<WalSummaryFile> {
    unimplemented!()
}

pub fn filter_wal_summaries(
    _wslist: Vec<WalSummaryFile>,
    _tli: TimeLineID,
    _start_lsn: XLogRecPtr,
    _end_lsn: XLogRecPtr,
) -> Vec<WalSummaryFile> {
    unimplemented!()
}

/// C returns bool + `*missing_lsn` out-param -> `(complete, missing_lsn)`.
pub fn wal_summaries_are_complete(
    _wslist: &[WalSummaryFile],
    _start_lsn: XLogRecPtr,
    _end_lsn: XLogRecPtr,
) -> (bool, XLogRecPtr) {
    unimplemented!()
}

/// `missing_ok` collapses into the `Option` (None means not found).
pub fn open_wal_summary_file(_ws: &WalSummaryFile, _missing_ok: bool) -> Option<File> {
    unimplemented!()
}

pub fn remove_wal_summary_if_older_than(_ws: &WalSummaryFile, _cutoff_time: i64) {
    unimplemented!()
}

/// io_callback_fn shape: returns the number of bytes read.
pub fn read_wal_summary(_wal_summary_io: &mut WalSummaryIO, _data: &mut [u8], _length: i32) -> i32 {
    unimplemented!()
}

/// io_callback_fn shape: returns the number of bytes written.
pub fn write_wal_summary(_wal_summary_io: &mut WalSummaryIO, _data: &[u8], _length: i32) -> i32 {
    unimplemented!()
}

/// C variadic printf-style error reporter; the `callback_arg`/format become a
/// formatted message at the call site. `// TODO(panic)`: maps to ereport.
pub fn report_wal_summary_error(_callback_arg: &mut WalSummaryIO, _msg: &str) {
    unimplemented!()
}
