//! port/win32/sys/un.h - Windows shim for <sys/un.h> (struct sockaddr_un).

use std::ffi::c_char;

/// Windows defines this structure in <afunix.h>, but not all tool chains have
/// the header yet, so we define it here for now.
#[repr(C)]
pub struct sockaddr_un {
    pub sun_family: u16,
    pub sun_path: [c_char; 108],
}
