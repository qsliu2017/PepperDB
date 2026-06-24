//! Translated from PostgreSQL src/include/access/xlogarchive.h

use crate::access::xlogdefs::{TimeLineID, XLogSegNo};

pub fn RestoreArchivedFile(
    _path: &mut str,
    _xlogfname: &str,
    _recovername: &str,
    _expected_size: i64,
    _cleanup_enabled: bool,
) -> bool {
    unimplemented!()
}

pub fn ExecuteRecoveryCommand(
    _command: &str,
    _command_name: &str,
    _fail_on_signal: bool,
    _wait_event_info: u32,
) {
    unimplemented!()
}

pub fn KeepFileRestoredFromArchive(_path: &str, _xlogfname: &str) {
    unimplemented!()
}

pub fn XLogArchiveNotify(_xlog: &str) {
    unimplemented!()
}

pub fn XLogArchiveNotifySeg(_segno: XLogSegNo, _tli: TimeLineID) {
    unimplemented!()
}

pub fn XLogArchiveForceDone(_xlog: &str) {
    unimplemented!()
}

pub fn XLogArchiveCheckDone(_xlog: &str) -> bool {
    unimplemented!()
}

pub fn XLogArchiveIsBusy(_xlog: &str) -> bool {
    unimplemented!()
}

pub fn XLogArchiveIsReady(_xlog: &str) -> bool {
    unimplemented!()
}

pub fn XLogArchiveIsReadyOrDone(_xlog: &str) -> bool {
    unimplemented!()
}

pub fn XLogArchiveCleanup(_xlog: &str) {
    unimplemented!()
}
