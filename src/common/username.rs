//! Translated from PostgreSQL src/include/common/username.h

/// Look up the effective user name. On failure returns the error string.
pub fn get_user_name() -> Result<String, String> {
    unimplemented!()
}

/// Look up the effective user name, exiting on failure.
pub fn get_user_name_or_exit(progname: &str) -> String {
    let _ = progname;
    unimplemented!()
}
