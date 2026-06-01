//! fe_utils/mbprint.h - Multibyte character printing support for frontend code

use std::ffi::{c_char, c_int, c_uchar};

use crate::c::Size;

// struct lineptr
#[repr(C)]
pub struct lineptr {
    pub ptr: *mut c_uchar,
    pub width: c_int,
}

pub unsafe fn mbvalidate(pwcs: *mut c_uchar, encoding: c_int) -> *mut c_uchar {
    unimplemented!()
}

pub unsafe fn pg_wcswidth(pwcs: *const c_char, len: Size, encoding: c_int) -> c_int {
    unimplemented!()
}

pub unsafe fn pg_wcsformat(
    pwcs: *const c_uchar,
    len: Size,
    encoding: c_int,
    lines: *mut lineptr,
    count: c_int,
) {
    unimplemented!()
}

pub unsafe fn pg_wcssize(
    pwcs: *const c_uchar,
    len: Size,
    encoding: c_int,
    result_width: *mut c_int,
    result_height: *mut c_int,
    result_format_size: *mut c_int,
) {
    unimplemented!()
}
