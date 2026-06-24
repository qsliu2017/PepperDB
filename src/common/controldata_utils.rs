//! Translated from PostgreSQL src/include/common/controldata_utils.h
//! Common code for pg_controldata output.

use crate::catalog::pg_control::ControlFileData;

/// Reads `$DataDir/global/pg_control`. The `*crc_ok_p` out-param folds into the
/// tuple (true if the CRC validated).
pub fn get_controlfile(_data_dir: &str) -> (ControlFileData, bool) {
    unimplemented!()
}

/// Like `get_controlfile`, but takes the exact control-file path.
pub fn get_controlfile_by_exact_path(_control_file_path: &str) -> (ControlFileData, bool) {
    unimplemented!()
}

pub fn update_controlfile(_data_dir: &str, _control_file: &ControlFileData, _do_sync: bool) {
    unimplemented!()
}
