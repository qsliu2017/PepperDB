//! Translated from PostgreSQL src/include/fe_utils/option_utils.h
//
// Frontend command-line option helpers. The bool-success + out-param idioms
// (option_parse_int, parse_sync_method) collapse to Option in the return.

use crate::common::file_utils::DataDirSyncMethod;

/// C: `typedef void (*help_handler) (const char *progname);`
pub type HelpHandler = fn(progname: &str);

pub fn handle_help_version_opts(args: &[String], fixed_progname: &str, hlp: HelpHandler) {
    unimplemented!()
}

/// C: `bool option_parse_int(optarg, optname, min, max, int *result)`.
/// Returns the parsed value, or None on parse/range failure.
pub fn option_parse_int(
    optarg: &str,
    optname: &str,
    min_range: i32,
    max_range: i32,
) -> Option<i32> {
    unimplemented!()
}

/// C: `bool parse_sync_method(optarg, DataDirSyncMethod *sync_method)`.
/// Returns the method, or None if `optarg` is not recognized.
pub fn parse_sync_method(optarg: &str) -> Option<DataDirSyncMethod> {
    unimplemented!()
}
