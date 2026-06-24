//! Translated from PostgreSQL src/include/utils/conffiles.h

/// recursion nesting depth for configuration files
pub const CONF_FILE_START_DEPTH: i32 = 0;
pub const CONF_FILE_MAX_DEPTH: i32 = 10;

pub fn absolute_config_location(location: &str, calling_file: &str) -> String {
    let _ = (location, calling_file);
    unimplemented!()
}

/// Returns the matching filenames; err_msg folded into the Result.
pub fn get_conf_files_in_dir(
    includedir: &str,
    calling_file: &str,
    elevel: i32,
) -> Result<Vec<String>, String> {
    let _ = (includedir, calling_file, elevel);
    unimplemented!()
}
