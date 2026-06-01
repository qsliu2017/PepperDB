//! port/win32_msvc/dirent.h - Windows (MSVC) native dirent implementation shims.
//!
//! Headers for port/dirent.c, win32 native implementation of dirent functions.

use crate::c::*;
use std::ffi::{c_char, c_int};

// MAX_PATH is a Windows system constant (windef.h); stubbed locally.
// TODO: Windows constant (windows.h MAX_PATH).
pub const MAX_PATH: usize = 260;

#[repr(C)]
pub struct dirent {
    pub d_ino: c_long,
    pub d_reclen: c_ushort,
    pub d_type: c_uchar,
    pub d_namlen: c_ushort,
    pub d_name: [c_char; MAX_PATH],
}

// Opaque directory stream handle (defined in port/dirent.c).
// TODO: opaque struct DIR (defined in C source).
pub enum DIR {}

pub unsafe fn opendir(_: *const c_char) -> *mut DIR {
    unimplemented!()
}

pub unsafe fn readdir(_: *mut DIR) -> *mut dirent {
    unimplemented!()
}

pub unsafe fn closedir(_: *mut DIR) -> c_int {
    unimplemented!()
}

// File types for 'd_type'.
pub const DT_UNKNOWN: c_int = 0;
pub const DT_FIFO: c_int = 1;
pub const DT_CHR: c_int = 2;
pub const DT_DIR: c_int = 4;
pub const DT_BLK: c_int = 6;
pub const DT_REG: c_int = 8;
pub const DT_LNK: c_int = 10;
pub const DT_SOCK: c_int = 12;
pub const DT_WHT: c_int = 14;
