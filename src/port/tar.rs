//! Translation of postgres/src/port/tar.c
//!   (declarations in postgres/src/include/pgtar.h).
//!
//! Functions for manipulating tarfile datastructures.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! The .c does `#include "c.h"`, `#include <sys/stat.h>`, and
//! `#include "pgtar.h"`.  From `c.h` we use the `Min` helper.  From
//! `<sys/stat.h>` we use the `S_ISDIR` test and the file-mode bits.  The header
//! `pgtar.h` supplies the `TAR_*` constants and the `tarError`/`tarHeaderOffset`/
//! `tarFileType` enums, all reproduced here.

use crate::prelude::*;

// strlcpy is the sibling port routine used to fill the fixed-width string
// fields of the header (the C source reaches it through port.h / c.h).
use crate::port::strlcpy::strlcpy;

// TODO(pg-port): the prelude does not export libc `strlen`; provide a private
// NUL-scanning helper matching C's `strlen` over a `const char *`.
//
// # Safety
// `s` must point to a valid NUL-terminated C string.
unsafe fn strlen(s: *const c_char) -> usize {
    let mut n: usize = 0;
    while *s.add(n) != 0 {
        n += 1;
    }
    n
}

// ---------------------------------------------------------------------------
// pgtar.h
// ---------------------------------------------------------------------------

pub const TAR_BLOCK_SIZE: usize = 512;

/// `enum tarError`
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum tarError {
    TAR_OK = 0,
    TAR_NAME_TOO_LONG,
    TAR_SYMLINK_TOO_LONG,
}
pub use tarError::*;

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
pub const TAR_OFFSET_NAME: usize = 0; /* 100 byte string */
pub const TAR_OFFSET_MODE: usize = 100; /* 8 byte tar number, excludes S_IFMT */
pub const TAR_OFFSET_UID: usize = 108; /* 8 byte tar number */
pub const TAR_OFFSET_GID: usize = 116; /* 8 byte tar number */
pub const TAR_OFFSET_SIZE: usize = 124; /* 8 byte tar number */
pub const TAR_OFFSET_MTIME: usize = 136; /* 12 byte tar number */
pub const TAR_OFFSET_CHECKSUM: usize = 148; /* 8 byte tar number */
pub const TAR_OFFSET_TYPEFLAG: usize = 156; /* 1 byte file type, see TAR_FILETYPE_* */
pub const TAR_OFFSET_LINKNAME: usize = 157; /* 100 byte string */
pub const TAR_OFFSET_MAGIC: usize = 257; /* "ustar" with terminating zero byte */
pub const TAR_OFFSET_VERSION: usize = 263; /* "00" */
pub const TAR_OFFSET_UNAME: usize = 265; /* 32 byte string */
pub const TAR_OFFSET_GNAME: usize = 297; /* 32 byte string */
pub const TAR_OFFSET_DEVMAJOR: usize = 329; /* 8 byte tar number */
pub const TAR_OFFSET_DEVMINOR: usize = 337; /* 8 byte tar number */
pub const TAR_OFFSET_PREFIX: usize = 345; /* 155 byte string */
/* last 12 bytes of the 512-byte block are unassigned */

// enum tarFileType
pub const TAR_FILETYPE_PLAIN: c_char = b'0' as c_char;
pub const TAR_FILETYPE_SYMLINK: c_char = b'2' as c_char;
pub const TAR_FILETYPE_DIRECTORY: c_char = b'5' as c_char;

// ---------------------------------------------------------------------------
// POSIX scalar types and <sys/stat.h> bits not yet provided by the crate.
//
// pgoff_t is PostgreSQL's portable off_t (a signed 64-bit file offset).  The
// remaining identity/time types mirror the POSIX definitions used on the
// non-WIN32 platforms PepperDB targets (Linux/Darwin).
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

// from <sys/stat.h>: the file-type mask and the directory file-type value.
const S_IFMT: mode_t = 0o170000;
const S_IFDIR: mode_t = 0o040000;

/// `S_ISDIR(m)` -- true when the mode designates a directory.
#[inline]
fn S_ISDIR(m: mode_t) -> bool {
    (m & S_IFMT) == S_IFDIR
}

/*
 * Compute the number of padding bytes required for an entry in a tar
 * archive. We must pad out to a multiple of TAR_BLOCK_SIZE. Since that's
 * a power of 2, we can use TYPEALIGN().
 */
#[inline]
pub fn tarPaddingBytesRequired(len: Size) -> Size {
    TYPEALIGN(TAR_BLOCK_SIZE, len) - len
}

// ---------------------------------------------------------------------------
// tar.c
// ---------------------------------------------------------------------------

/*
 * Print a numeric field in a tar header.  The field starts at *s and is of
 * length len; val is the value to be written.
 *
 * Per POSIX, the way to write a number is in octal with leading zeroes and
 * one trailing space (or NUL, but we use space) at the end of the specified
 * field width.
 *
 * However, the given value may not fit in the available space in octal form.
 * If that's true, we use the GNU extension of writing \200 followed by the
 * number in base-256 form (ie, stored in binary MSB-first).  (Note: here we
 * support only non-negative numbers, so we don't worry about the GNU rules
 * for handling negative numbers.)
 *
 * # Safety
 * `s` must be valid for `len` bytes.
 */
pub unsafe fn print_tar_number(s: *mut c_char, mut len: c_int, mut val: uint64) {
    // C: if (val < (((uint64) 1) << ((len - 1) * 3)))
    if val < ((1u64) << ((len - 1) * 3)) {
        /* Use octal with trailing space */
        len -= 1;
        *s.add(len as usize) = b' ' as c_char;
        // C: while (len) { s[--len] = (val & 7) + '0'; val >>= 3; }
        while len != 0 {
            len -= 1;
            *s.add(len as usize) = ((val & 7) as c_char).wrapping_add(b'0' as c_char);
            val >>= 3;
        }
    } else {
        /* Use base-256 with leading \200 */
        *s.add(0) = 0o200u8 as c_char;
        // C: while (len > 1) { s[--len] = (val & 255); val >>= 8; }
        while len > 1 {
            len -= 1;
            *s.add(len as usize) = (val & 255) as c_char;
            val >>= 8;
        }
    }
}

/*
 * Read a numeric field in a tar header.  The field starts at *s and is of
 * length len.
 *
 * The POSIX-approved format for a number is octal, ending with a space or
 * NUL.  However, for values that don't fit, we recognize the GNU extension
 * of \200 followed by the number in base-256 form (ie, stored in binary
 * MSB-first).  (Note: here we support only non-negative numbers, so we don't
 * worry about the GNU rules for handling negative numbers.)
 *
 * # Safety
 * `s` must be valid for `len` bytes.
 */
pub unsafe fn read_tar_number(s: *const c_char, mut len: c_int) -> uint64 {
    let mut result: uint64 = 0;
    let mut s = s;

    if *s == 0o200u8 as c_char {
        /* base-256 */
        // C: while (--len) { result <<= 8; result |= (unsigned char) (*++s); }
        loop {
            len -= 1;
            if len == 0 {
                break;
            }
            result <<= 8;
            s = s.add(1);
            result |= (*s as c_uchar) as uint64;
        }
    } else {
        /* octal */
        // C: while (len-- && *s >= '0' && *s <= '7')
        while len != 0 && *s >= b'0' as c_char && *s <= b'7' as c_char {
            len -= 1;
            result <<= 3;
            result |= (*s - b'0' as c_char) as uint64;
            s = s.add(1);
        }
    }
    result
}

/*
 * Calculate the tar checksum for a header. The header is assumed to always
 * be 512 bytes, per the tar standard.
 *
 * # Safety
 * `header` must be valid for 512 bytes.
 */
pub unsafe fn tarChecksum(header: *const c_char) -> c_int {
    let mut sum: c_int;

    /*
     * Per POSIX, the checksum is the simple sum of all bytes in the header,
     * treating the bytes as unsigned, and treating the checksum field (at
     * offset 148) as though it contained 8 spaces.
     */
    sum = 8 * b' ' as c_int; /* presumed value for checksum field */
    // C: for (i = 0; i < 512; i++) if (i < 148 || i >= 156) sum += 0xFF & header[i];
    let mut i: c_int = 0;
    while i < 512 {
        if i < 148 || i >= 156 {
            sum += 0xFF & *header.add(i as usize) as c_int;
        }
        i += 1;
    }
    sum
}

/*
 * Fill in the buffer pointed to by h with a tar format header. This buffer
 * must always have space for 512 characters, which is a requirement of
 * the tar format.
 *
 * # Safety
 * `h` must be valid for 512 bytes; `filename` must be a NUL-terminated C
 * string; `linktarget`, if non-NULL, must be a NUL-terminated C string.
 */
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
    if strlen(filename) > 99 {
        return TAR_NAME_TOO_LONG;
    }

    if !linktarget.is_null() && strlen(linktarget) > 99 {
        return TAR_SYMLINK_TOO_LONG;
    }

    h.write_bytes(0, TAR_BLOCK_SIZE);

    /* Name 100 */
    strlcpy(h.add(TAR_OFFSET_NAME), filename, 100);
    if !linktarget.is_null() || S_ISDIR(mode) {
        /*
         * We only support symbolic links to directories, and this is
         * indicated in the tar format by adding a slash at the end of the
         * name, the same as for regular directories.
         */
        let mut flen: c_int = strlen(filename) as c_int;

        flen = Min(flen, 99);
        *h.add(flen as usize) = b'/' as c_char;
        *h.add(flen as usize + 1) = b'\0' as c_char;
    }

    /* Mode 8 - this doesn't include the file type bits (S_IFMT)  */
    print_tar_number(h.add(TAR_OFFSET_MODE), 8, (mode & 0o7777) as uint64);

    /* User ID 8 */
    print_tar_number(h.add(TAR_OFFSET_UID), 8, uid as uint64);

    /* Group 8 */
    print_tar_number(h.add(TAR_OFFSET_GID), 8, gid as uint64);

    /* File size 12 */
    if !linktarget.is_null() || S_ISDIR(mode) {
        /* Symbolic link or directory has size zero */
        print_tar_number(h.add(TAR_OFFSET_SIZE), 12, 0);
    } else {
        print_tar_number(h.add(TAR_OFFSET_SIZE), 12, size as uint64);
    }

    /* Mod Time 12 */
    print_tar_number(h.add(TAR_OFFSET_MTIME), 12, mtime as uint64);

    /* Checksum 8 cannot be calculated until we've filled all other fields */

    if !linktarget.is_null() {
        /* Type - Symbolic link */
        *h.add(TAR_OFFSET_TYPEFLAG) = TAR_FILETYPE_SYMLINK;
        /* Link Name 100 */
        strlcpy(h.add(TAR_OFFSET_LINKNAME), linktarget, 100);
    } else if S_ISDIR(mode) {
        /* Type - directory */
        *h.add(TAR_OFFSET_TYPEFLAG) = TAR_FILETYPE_DIRECTORY;
    } else {
        /* Type - regular file */
        *h.add(TAR_OFFSET_TYPEFLAG) = TAR_FILETYPE_PLAIN;
    }

    /* Magic 6 */
    // C: strcpy(&h[TAR_OFFSET_MAGIC], "ustar");  (copies the 5 chars + NUL)
    {
        let magic: &[u8; 6] = b"ustar\0";
        core::ptr::copy_nonoverlapping(
            magic.as_ptr() as *const c_char,
            h.add(TAR_OFFSET_MAGIC),
            magic.len(),
        );
    }

    /* Version 2 */
    // C: memcpy(&h[TAR_OFFSET_VERSION], "00", 2);
    core::ptr::copy_nonoverlapping(
        b"00".as_ptr() as *const c_char,
        h.add(TAR_OFFSET_VERSION),
        2,
    );

    /* User 32 */
    /* XXX: Do we need to care about setting correct username? */
    strlcpy(h.add(TAR_OFFSET_UNAME), c"postgres".as_ptr(), 32);

    /* Group 32 */
    /* XXX: Do we need to care about setting correct group name? */
    strlcpy(h.add(TAR_OFFSET_GNAME), c"postgres".as_ptr(), 32);

    /* Major Dev 8 */
    print_tar_number(h.add(TAR_OFFSET_DEVMAJOR), 8, 0);

    /* Minor Dev 8 */
    print_tar_number(h.add(TAR_OFFSET_DEVMINOR), 8, 0);

    /* Prefix 155 - not used, leave as nulls */

    /* Finally, compute and insert the checksum */
    print_tar_number(h.add(TAR_OFFSET_CHECKSUM), 8, tarChecksum(h) as uint64);

    TAR_OK
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn print_and_read_octal_roundtrip() {
        unsafe {
            let mut buf = [0u8 as c_char; 12];
            // 12-byte field can hold up to 8 octal digits trivially.
            print_tar_number(buf.as_mut_ptr(), 12, 0o644);
            // last byte is a trailing space
            assert_eq!(buf[11], b' ' as c_char);
            let v = read_tar_number(buf.as_ptr(), 12);
            assert_eq!(v, 0o644);
        }
    }

    #[test]
    fn print_and_read_base256_for_large_values() {
        unsafe {
            // A value too large to fit in 8 octal digits forces base-256.
            // 8-byte field holds octal values < (1 << 21) = 0o10000000.
            let big: uint64 = (1u64 << 40) + 12345;
            let mut buf = [0u8 as c_char; 8];
            print_tar_number(buf.as_mut_ptr(), 8, big);
            assert_eq!(buf[0], 0o200u8 as c_char); // GNU base-256 marker
            let v = read_tar_number(buf.as_ptr(), 8);
            assert_eq!(v, big);
        }
    }

    #[test]
    fn header_checksum_is_consistent() {
        unsafe {
            let mut h = [0u8 as c_char; TAR_BLOCK_SIZE];
            let rc = tarCreateHeader(
                h.as_mut_ptr(),
                c"base/1234".as_ptr(),
                core::ptr::null(),
                2048,
                0o600,
                0,
                0,
                0,
            );
            assert_eq!(rc, TAR_OK);
            // magic "ustar"
            assert_eq!(h[TAR_OFFSET_MAGIC], b'u' as c_char);
            assert_eq!(h[TAR_OFFSET_TYPEFLAG], TAR_FILETYPE_PLAIN);
            // The stored checksum (read back) must equal a fresh tarChecksum().
            let stored = read_tar_number(h.as_ptr().add(TAR_OFFSET_CHECKSUM), 8);
            assert_eq!(stored as c_int, tarChecksum(h.as_ptr()));
        }
    }

    #[test]
    fn directory_gets_trailing_slash_and_zero_size() {
        unsafe {
            let mut h = [0u8 as c_char; TAR_BLOCK_SIZE];
            let rc = tarCreateHeader(
                h.as_mut_ptr(),
                c"some/dir".as_ptr(),
                core::ptr::null(),
                999,
                S_IFDIR | 0o700,
                0,
                0,
                0,
            );
            assert_eq!(rc, TAR_OK);
            assert_eq!(h[TAR_OFFSET_TYPEFLAG], TAR_FILETYPE_DIRECTORY);
            // name "some/dir" is 8 chars, so slash lands at index 8
            assert_eq!(h[8], b'/' as c_char);
            // size field reads as zero for a directory
            let sz = read_tar_number(h.as_ptr().add(TAR_OFFSET_SIZE), 12);
            assert_eq!(sz, 0);
        }
    }

    #[test]
    fn name_too_long_is_rejected() {
        unsafe {
            let long = [b'a' as c_char; 101];
            // build a NUL-terminated 100-char-plus name
            let mut name = [0u8 as c_char; 102];
            core::ptr::copy_nonoverlapping(long.as_ptr(), name.as_mut_ptr(), 100);
            name[100] = 0;
            let _ = &name; // ensure terminated
            let mut h = [0u8 as c_char; TAR_BLOCK_SIZE];
            let rc = tarCreateHeader(
                h.as_mut_ptr(),
                name.as_ptr(),
                core::ptr::null(),
                0,
                0o600,
                0,
                0,
                0,
            );
            assert_eq!(rc, TAR_NAME_TOO_LONG);
        }
    }
}
