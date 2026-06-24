//! Translated from PostgreSQL src/include/pgtar.h
//! Functions for manipulating tarfile datastructures (src/port/tar.c).

pub const TAR_BLOCK_SIZE: usize = 512;

/// C: `enum tarError`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum tarError {
    TAR_OK = 0,
    TAR_NAME_TOO_LONG,
    TAR_SYMLINK_TOO_LONG,
}

/// Offsets of fields within a 512-byte tar header (on-disk layout).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum tarHeaderOffset {
    TAR_OFFSET_NAME = 0,        // 100 byte string
    TAR_OFFSET_MODE = 100,      // 8 byte tar number, excludes S_IFMT
    TAR_OFFSET_UID = 108,       // 8 byte tar number
    TAR_OFFSET_GID = 116,       // 8 byte tar number
    TAR_OFFSET_SIZE = 124,      // 8 byte tar number
    TAR_OFFSET_MTIME = 136,     // 12 byte tar number
    TAR_OFFSET_CHECKSUM = 148,  // 8 byte tar number
    TAR_OFFSET_TYPEFLAG = 156,  // 1 byte file type
    TAR_OFFSET_LINKNAME = 157,  // 100 byte string
    TAR_OFFSET_MAGIC = 257,     // "ustar" + NUL
    TAR_OFFSET_VERSION = 263,   // "00"
    TAR_OFFSET_UNAME = 265,     // 32 byte string
    TAR_OFFSET_GNAME = 297,     // 32 byte string
    TAR_OFFSET_DEVMAJOR = 329,  // 8 byte tar number
    TAR_OFFSET_DEVMINOR = 337,  // 8 byte tar number
    TAR_OFFSET_PREFIX = 345,    // 155 byte string
}

/// C: `enum tarFileType` (POSIX file type codes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum tarFileType {
    TAR_FILETYPE_PLAIN = b'0' as isize,
    TAR_FILETYPE_PLAIN_OLD = b'\0' as isize, // backwards compatibility
    TAR_FILETYPE_SYMLINK = b'2' as isize,
    TAR_FILETYPE_DIRECTORY = b'5' as isize,
    TAR_FILETYPE_PAX_EXTENDED = b'x' as isize,
    TAR_FILETYPE_PAX_EXTENDED_GLOBAL = b'g' as isize,
}

/// status int -> Result. Fills the 512-byte header buffer `h`.
pub fn tarCreateHeader(
    _h: &mut [u8],
    _filename: &str,
    _linktarget: Option<&str>,
    _size: i64,  // pgoff_t
    _mode: u32,  // mode_t
    _uid: u32,   // uid_t
    _gid: u32,   // gid_t
    _mtime: i64, // time_t
) -> Result<(), tarError> {
    unimplemented!()
}

pub fn read_tar_number(_s: &[u8], _len: i32) -> u64 {
    unimplemented!()
}

pub fn print_tar_number(_s: &mut [u8], _len: i32, _val: u64) {
    unimplemented!()
}

pub fn tarChecksum(_header: &[u8]) -> i32 {
    unimplemented!()
}

pub fn isValidTarHeader(_header: &[u8]) -> bool {
    unimplemented!()
}

/// static inline: padding to round `len` up to a TAR_BLOCK_SIZE multiple.
pub const fn tarPaddingBytesRequired(len: usize) -> usize {
    // TYPEALIGN(TAR_BLOCK_SIZE, len) - len; TAR_BLOCK_SIZE is a power of 2.
    (len.wrapping_neg()) & (TAR_BLOCK_SIZE - 1)
}
