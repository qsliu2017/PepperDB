//! Translated from PostgreSQL src/include/access/timeline.h
//! Reading and writing timeline history files. In-memory (List -> Vec).

use crate::access::xlogdefs::{TimeLineID, XLogRecPtr};

/// One piece of WAL belonging to the server's timeline history. All WAL between
/// `begin` (inclusive) and `end` (exclusive) belongs to timeline `tli`;
/// `end == InvalidXLogRecPtr` means infinity.
#[derive(Debug, Clone, Copy)]
pub struct TimeLineHistoryEntry {
    pub tli: TimeLineID,
    pub begin: XLogRecPtr, // inclusive
    pub end: XLogRecPtr,   // exclusive, InvalidXLogRecPtr means infinity
}

pub fn readTimeLineHistory(_targetTLI: TimeLineID) -> Vec<TimeLineHistoryEntry> {
    unimplemented!()
}

pub fn existsTimeLineHistory(_probeTLI: TimeLineID) -> bool {
    unimplemented!()
}

pub fn findNewestTimeLine(_startTLI: TimeLineID) -> TimeLineID {
    unimplemented!()
}

pub fn writeTimeLineHistory(
    _newTLI: TimeLineID,
    _parentTLI: TimeLineID,
    _switchpoint: XLogRecPtr,
    _reason: &str,
) {
    unimplemented!()
}

pub fn writeTimeLineHistoryFile(_tli: TimeLineID, _content: &[u8]) {
    unimplemented!()
}

pub fn restoreTimeLineHistoryFiles(_begin: TimeLineID, _end: TimeLineID) {
    unimplemented!()
}

pub fn tliInHistory(_tli: TimeLineID, _expectedTLEs: &[TimeLineHistoryEntry]) -> bool {
    unimplemented!()
}

pub fn tliOfPointInHistory(_ptr: XLogRecPtr, _history: &[TimeLineHistoryEntry]) -> TimeLineID {
    unimplemented!()
}

/// Returns (switch point, next TLI) (C returns the LSN and fills *nextTLI).
pub fn tliSwitchPoint(
    _tli: TimeLineID,
    _history: &[TimeLineHistoryEntry],
) -> (XLogRecPtr, TimeLineID) {
    unimplemented!()
}
