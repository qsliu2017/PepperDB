//! pgtar.h - Functions for manipulating tarfile datastructures (src/port/tar.c)

use std::ffi::{c_char, c_int, c_long, c_uint};

use crate::c::{int64, uint64, Size, TYPEALIGN};

pub const TAR_BLOCK_SIZE: c_int = 512;

// ---------------------------------------------------------------------------
// POSIX scalar types not yet provided crate-wide; mirror the definitions used
// on the non-WIN32 platforms PepperDB targets.  TODO: dedup (also in
// src/port/tar.rs, src/fe_utils/astreamer.rs).
// ---------------------------------------------------------------------------
#[allow(non_camel_case_types)]
pub type pgoff_t = int64;
#[allow(non_camel_case_types)]
pub type mode_t = c_uint;
#[allow(non_camel_case_types)]
pub type uid_t = c_uint;
#[allow(non_camel_case_types)]
pub type gid_t = c_uint;
#[allow(non_camel_case_types)]
pub type time_t = c_long;

// enum tarError
#[allow(non_camel_case_types)]
pub type tarError = c_int;
pub const TAR_OK: tarError = 0;
pub const TAR_NAME_TOO_LONG: tarError = 1;
pub const TAR_SYMLINK_TOO_LONG: tarError = 2;

/*
 * Offsets of fields within a 512-byte tar header.
 *
 * "tar number" values should be generated using print_tar_number() and can be
 * read using read_tar_number(). Fields that contain strings are generally
 * both filled and read using strlcpy().
 *
 * The value for the checksum field can be computed using tarChecksum().
 *
 * Some fields are not used by PostgreSQL; see tarCreateHeader().
 */
// enum tarHeaderOffset
#[allow(non_camel_case_types)]
pub type tarHeaderOffset = c_int;
pub const TAR_OFFSET_NAME: tarHeaderOffset = 0; /* 100 byte string */
pub const TAR_OFFSET_MODE: tarHeaderOffset = 100; /* 8 byte tar number, excludes S_IFMT */
pub const TAR_OFFSET_UID: tarHeaderOffset = 108; /* 8 byte tar number */
pub const TAR_OFFSET_GID: tarHeaderOffset = 116; /* 8 byte tar number */
pub const TAR_OFFSET_SIZE: tarHeaderOffset = 124; /* 8 byte tar number */
pub const TAR_OFFSET_MTIME: tarHeaderOffset = 136; /* 12 byte tar number */
pub const TAR_OFFSET_CHECKSUM: tarHeaderOffset = 148; /* 8 byte tar number */
pub const TAR_OFFSET_TYPEFLAG: tarHeaderOffset = 156; /* 1 byte file type, see TAR_FILETYPE_* */
pub const TAR_OFFSET_LINKNAME: tarHeaderOffset = 157; /* 100 byte string */
pub const TAR_OFFSET_MAGIC: tarHeaderOffset = 257; /* "ustar" with terminating zero byte */
pub const TAR_OFFSET_VERSION: tarHeaderOffset = 263; /* "00" */
pub const TAR_OFFSET_UNAME: tarHeaderOffset = 265; /* 32 byte string */
pub const TAR_OFFSET_GNAME: tarHeaderOffset = 297; /* 32 byte string */
pub const TAR_OFFSET_DEVMAJOR: tarHeaderOffset = 329; /* 8 byte tar number */
pub const TAR_OFFSET_DEVMINOR: tarHeaderOffset = 337; /* 8 byte tar number */
pub const TAR_OFFSET_PREFIX: tarHeaderOffset = 345; /* 155 byte string */
/* last 12 bytes of the 512-byte block are unassigned */

// enum tarFileType
#[allow(non_camel_case_types)]
pub type tarFileType = c_int;
pub const TAR_FILETYPE_PLAIN: tarFileType = b'0' as c_int;
pub const TAR_FILETYPE_SYMLINK: tarFileType = b'2' as c_int;
pub const TAR_FILETYPE_DIRECTORY: tarFileType = b'5' as c_int;

pub unsafe fn tarCreateHeader(
    h: *mut c_char,
    filename: *const c_char,
    linktarget: *const c_char,
    size: pgoff_t,
    mode: mode_t,
    uid: uid_t,
    gid: gid_t,
    mtime: time_t,
) -> tarError {
    unimplemented!()
}

pub unsafe fn read_tar_number(s: *const c_char, len: c_int) -> uint64 {
    unimplemented!()
}

pub unsafe fn print_tar_number(s: *mut c_char, len: c_int, val: uint64) {
    unimplemented!()
}

pub unsafe fn tarChecksum(header: *mut c_char) -> c_int {
    unimplemented!()
}

/*
 * Compute the number of padding bytes required for an entry in a tar
 * archive. We must pad out to a multiple of TAR_BLOCK_SIZE. Since that's
 * a power of 2, we can use TYPEALIGN().
 */
#[inline]
pub fn tarPaddingBytesRequired(len: Size) -> Size {
    TYPEALIGN(TAR_BLOCK_SIZE as Size, len) - len
}
