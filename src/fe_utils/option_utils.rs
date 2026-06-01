//! fe_utils/option_utils.h - Command line option processing facilities for frontend code

use std::ffi::{c_char, c_int};

use crate::common::file_utils::DataDirSyncMethod;

// typedef void (*help_handler) (const char *progname);
pub type help_handler = Option<unsafe extern "C" fn(progname: *const c_char)>;

pub unsafe fn handle_help_version_opts(
    argc: c_int,
    argv: *mut *mut c_char,
    fixed_progname: *const c_char,
    hlp: help_handler,
) {
    unimplemented!()
}

pub unsafe fn option_parse_int(
    optarg: *const c_char,
    optname: *const c_char,
    min_range: c_int,
    max_range: c_int,
    result: *mut c_int,
) -> bool {
    unimplemented!()
}

pub unsafe fn parse_sync_method(
    optarg: *const c_char,
    sync_method: *mut DataDirSyncMethod,
) -> bool {
    unimplemented!()
}
