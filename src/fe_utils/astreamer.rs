//! fe_utils/astreamer.h - archive streamer interface for frontend code.

use std::ffi::{c_char, c_int, c_void};

use crate::common::compression::pg_compress_specification;
use crate::lib::stringinfo::{appendBinaryStringInfo, StringInfoData};

// MAXPGPATH is defined in several modules already; redeclare locally to match.
// TODO: dedup with crate::port pg_config_paths once a canonical home exists.
pub const MAXPGPATH: usize = 1024;

// Frontend stdio FILE handle. Treated as opaque.
// TODO: dedup with crate::commands::copyfrom_internal::FILE.
pub type FILE = c_void;

// POSIX file-attribute types (from <sys/types.h>); local stubs.
// TODO: dedup with crate::port::tar.
pub type pgoff_t = i64;
pub type mode_t = c_int;
pub type uid_t = c_int;
pub type gid_t = c_int;

/*
 * Each chunk of archive data passed to a astreamer is classified into one
 * of these categories.
 */
pub type astreamer_archive_context = c_int;
pub const ASTREAMER_UNKNOWN: astreamer_archive_context = 0;
pub const ASTREAMER_MEMBER_HEADER: astreamer_archive_context = 1;
pub const ASTREAMER_MEMBER_CONTENTS: astreamer_archive_context = 2;
pub const ASTREAMER_MEMBER_TRAILER: astreamer_archive_context = 3;
pub const ASTREAMER_ARCHIVE_TRAILER: astreamer_archive_context = 4;

/*
 * Each chunk of data that is classified as ASTREAMER_MEMBER_HEADER,
 * ASTREAMER_MEMBER_CONTENTS, or ASTREAMER_MEMBER_TRAILER should also
 * pass a pointer to an instance of this struct.
 */
#[repr(C)]
pub struct astreamer_member {
    pub pathname: [c_char; MAXPGPATH],
    pub size: pgoff_t,
    pub mode: mode_t,
    pub uid: uid_t,
    pub gid: gid_t,
    pub is_directory: bool,
    pub is_link: bool,
    pub linktarget: [c_char; MAXPGPATH],
}

/*
 * Generally, each type of astreamer will define its own struct, but the
 * first element should be 'astreamer base'. A astreamer that does not
 * require any additional private data could use this structure directly.
 */
#[repr(C)]
pub struct astreamer {
    pub bbs_ops: *const astreamer_ops,
    pub bbs_next: *mut astreamer,
    pub bbs_buffer: StringInfoData,
}

/*
 * There are three callbacks for a astreamer.
 */
#[repr(C)]
#[allow(improper_ctypes)]
pub struct astreamer_ops {
    pub content: Option<
        unsafe extern "C" fn(
            streamer: *mut astreamer,
            member: *mut astreamer_member,
            data: *const c_char,
            len: c_int,
            context: astreamer_archive_context,
        ),
    >,
    pub finalize: Option<unsafe extern "C" fn(streamer: *mut astreamer)>,
    pub free: Option<unsafe extern "C" fn(streamer: *mut astreamer)>,
}

/* Send some content to a astreamer. */
#[inline]
pub unsafe fn astreamer_content(
    streamer: *mut astreamer,
    member: *mut astreamer_member,
    data: *const c_char,
    len: c_int,
    context: astreamer_archive_context,
) {
    debug_assert!(!streamer.is_null());
    ((*(*streamer).bbs_ops).content.unwrap())(streamer, member, data, len, context);
}

/* Finalize a astreamer. */
#[inline]
pub unsafe fn astreamer_finalize(streamer: *mut astreamer) {
    debug_assert!(!streamer.is_null());
    ((*(*streamer).bbs_ops).finalize.unwrap())(streamer);
}

/* Free a astreamer. */
#[inline]
pub unsafe fn astreamer_free(streamer: *mut astreamer) {
    debug_assert!(!streamer.is_null());
    ((*(*streamer).bbs_ops).free.unwrap())(streamer);
}

/*
 * This is a convenience method for use when implementing a astreamer; it is
 * not for use by outside callers. It adds the amount of data specified by
 * 'nbytes' to the astreamer's buffer and adjusts '*len' and '*data'
 * accordingly.
 */
#[inline]
pub unsafe fn astreamer_buffer_bytes(
    streamer: *mut astreamer,
    data: *mut *const c_char,
    len: *mut c_int,
    nbytes: c_int,
) {
    debug_assert!(nbytes <= *len);

    appendBinaryStringInfo(
        &mut (*streamer).bbs_buffer,
        *data as *const c_void,
        nbytes,
    );
    *len -= nbytes;
    *data = (*data).add(nbytes as usize);
}

/*
 * This is a convenience method for use when implementing a astreamer; it is
 * not for use by outsider callers. It attempts to add enough data to the
 * astreamer's buffer to reach a length of target_bytes and adjusts '*len'
 * and '*data' accordingly. It returns true if the target length has been
 * reached and false otherwise.
 */
#[inline]
pub unsafe fn astreamer_buffer_until(
    streamer: *mut astreamer,
    data: *mut *const c_char,
    len: *mut c_int,
    target_bytes: c_int,
) -> bool {
    let buflen: c_int = (*streamer).bbs_buffer.len;

    if buflen >= target_bytes {
        /* Target length already reached; nothing to do. */
        return true;
    }

    if buflen + *len < target_bytes {
        /* Not enough data to reach target length; buffer all of it. */
        astreamer_buffer_bytes(streamer, data, len, *len);
        return false;
    }

    /* Buffer just enough to reach the target length. */
    astreamer_buffer_bytes(streamer, data, len, target_bytes - buflen);
    true
}

/*
 * Functions for creating astreamer objects of various types.
 */
pub unsafe fn astreamer_plain_writer_new(
    pathname: *mut c_char,
    file: *mut FILE,
) -> *mut astreamer {
    unimplemented!()
}

pub unsafe fn astreamer_gzip_writer_new(
    pathname: *mut c_char,
    file: *mut FILE,
    compress: *mut pg_compress_specification,
) -> *mut astreamer {
    unimplemented!()
}

pub unsafe fn astreamer_extractor_new(
    basepath: *const c_char,
    link_map: Option<unsafe extern "C" fn(*const c_char) -> *const c_char>,
    report_output_file: Option<unsafe extern "C" fn(*const c_char)>,
) -> *mut astreamer {
    unimplemented!()
}

pub unsafe fn astreamer_gzip_decompressor_new(next: *mut astreamer) -> *mut astreamer {
    unimplemented!()
}

pub unsafe fn astreamer_lz4_compressor_new(
    next: *mut astreamer,
    compress: *mut pg_compress_specification,
) -> *mut astreamer {
    unimplemented!()
}

pub unsafe fn astreamer_lz4_decompressor_new(next: *mut astreamer) -> *mut astreamer {
    unimplemented!()
}

pub unsafe fn astreamer_zstd_compressor_new(
    next: *mut astreamer,
    compress: *mut pg_compress_specification,
) -> *mut astreamer {
    unimplemented!()
}

pub unsafe fn astreamer_zstd_decompressor_new(next: *mut astreamer) -> *mut astreamer {
    unimplemented!()
}

pub unsafe fn astreamer_tar_parser_new(next: *mut astreamer) -> *mut astreamer {
    unimplemented!()
}

pub unsafe fn astreamer_tar_terminator_new(next: *mut astreamer) -> *mut astreamer {
    unimplemented!()
}

pub unsafe fn astreamer_tar_archiver_new(next: *mut astreamer) -> *mut astreamer {
    unimplemented!()
}
