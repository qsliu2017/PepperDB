//! port/win32/sys/resource.h - Replacement for <sys/resource.h> for Windows.

use crate::port::win32::sys::time::timeval; // for struct timeval

use std::ffi::c_int;

pub const RUSAGE_SELF: c_int = 0;
pub const RUSAGE_CHILDREN: c_int = -1;

#[repr(C)]
pub struct rusage {
    pub ru_utime: timeval, // user time used
    pub ru_stime: timeval, // system time used
}

pub unsafe fn getrusage(who: c_int, rusage: *mut rusage) -> c_int {
    let _ = (who, rusage);
    unimplemented!()
}
