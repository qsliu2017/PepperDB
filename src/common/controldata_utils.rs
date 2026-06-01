//! common/controldata_utils.h - common code for pg_controldata output.

use std::ffi::{c_char, c_void};

// catalog/pg_control.h not yet ported; minimal opaque stub.
// TODO: dedup -> use crate::catalog::pg_control::ControlFileData when it lands.
pub type ControlFileData = c_void;

pub unsafe fn get_controlfile(
    DataDir: *const c_char,
    crc_ok_p: *mut bool,
) -> *mut ControlFileData {
    unimplemented!()
}

pub unsafe fn get_controlfile_by_exact_path(
    ControlFilePath: *const c_char,
    crc_ok_p: *mut bool,
) -> *mut ControlFileData {
    unimplemented!()
}

pub unsafe fn update_controlfile(
    DataDir: *const c_char,
    ControlFile: *mut ControlFileData,
    do_sync: bool,
) {
    unimplemented!()
}
