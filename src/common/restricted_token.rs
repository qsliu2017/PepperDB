//! common/restricted_token.h - helper routine to ensure restricted token on Windows.

/*
 * On Windows make sure that we are running with a restricted token,
 * On other platforms do nothing.
 */
pub unsafe fn get_restricted_token() {
    unimplemented!()
}

// NOTE: The WIN32-only `CreateRestrictedProcess` prototype, which returns a
// Windows HANDLE and takes a PROCESS_INFORMATION*, is omitted because those
// Windows-specific types are not available in this port.
