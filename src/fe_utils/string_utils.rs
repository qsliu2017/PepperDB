//! fe_utils/string_utils.h - String-processing utility routines for frontend code

use std::ffi::{c_char, c_int, c_void};

use crate::c::Size;

// TODO: dedup - frontend/libpq types not yet defined in src/; stub locally.
pub type PGconn = c_void;
pub type PGresult = c_void;
pub type PQExpBuffer = *mut PQExpBufferData;
pub type PQExpBufferData = c_void;

/* Global variables controlling behavior of fmtId() and fmtQualifiedId() */
pub static mut quote_all_identifiers: c_int = 0;
pub static mut getLocalPQExpBuffer: Option<unsafe extern "C" fn() -> PQExpBuffer> = None;

/* Functions */
pub unsafe fn fmtId(rawid: *const c_char) -> *const c_char {
    unimplemented!()
}

pub unsafe fn fmtIdEnc(rawid: *const c_char, encoding: c_int) -> *const c_char {
    unimplemented!()
}

pub unsafe fn fmtQualifiedId(schema: *const c_char, id: *const c_char) -> *const c_char {
    unimplemented!()
}

pub unsafe fn fmtQualifiedIdEnc(
    schema: *const c_char,
    id: *const c_char,
    encoding: c_int,
) -> *const c_char {
    unimplemented!()
}

pub unsafe fn setFmtEncoding(encoding: c_int) {
    unimplemented!()
}

pub unsafe fn formatPGVersionNumber(
    version_number: c_int,
    include_minor: bool,
    buf: *mut c_char,
    buflen: Size,
) -> *mut c_char {
    unimplemented!()
}

pub unsafe fn appendStringLiteral(
    buf: PQExpBuffer,
    str: *const c_char,
    encoding: c_int,
    std_strings: bool,
) {
    unimplemented!()
}

pub unsafe fn appendStringLiteralConn(buf: PQExpBuffer, str: *const c_char, conn: *mut PGconn) {
    unimplemented!()
}

pub unsafe fn appendStringLiteralDQ(buf: PQExpBuffer, str: *const c_char, dqprefix: *const c_char) {
    unimplemented!()
}

pub unsafe fn appendByteaLiteral(
    buf: PQExpBuffer,
    str: *const c_char,
    length: Size,
    std_strings: bool,
) {
    unimplemented!()
}

pub unsafe fn appendShellString(buf: PQExpBuffer, str: *const c_char) {
    unimplemented!()
}

pub unsafe fn appendShellStringNoError(buf: PQExpBuffer, str: *const c_char) -> bool {
    unimplemented!()
}

pub unsafe fn appendConnStrVal(buf: PQExpBuffer, str: *const c_char) {
    unimplemented!()
}

pub unsafe fn appendPsqlMetaConnect(buf: PQExpBuffer, dbname: *const c_char) {
    unimplemented!()
}

pub unsafe fn parsePGArray(
    atext: *const c_char,
    itemarray: *mut *mut *mut c_char,
    nitems: *mut c_int,
) -> bool {
    unimplemented!()
}

pub unsafe fn appendPGArray(buffer: PQExpBuffer, value: *const c_char) {
    unimplemented!()
}

pub unsafe fn appendReloptionsArray(
    buffer: PQExpBuffer,
    reloptions: *const c_char,
    prefix: *const c_char,
    encoding: c_int,
    std_strings: bool,
) -> bool {
    unimplemented!()
}

pub unsafe fn processSQLNamePattern(
    conn: *mut PGconn,
    buf: PQExpBuffer,
    pattern: *const c_char,
    have_where: bool,
    force_escape: bool,
    schemavar: *const c_char,
    namevar: *const c_char,
    altnamevar: *const c_char,
    visibilityrule: *const c_char,
    dbnamebuf: PQExpBuffer,
    dotcnt: *mut c_int,
) -> bool {
    unimplemented!()
}

pub unsafe fn patternToSQLRegex(
    encoding: c_int,
    dbnamebuf: PQExpBuffer,
    schemabuf: PQExpBuffer,
    namebuf: PQExpBuffer,
    pattern: *const c_char,
    force_escape: bool,
    want_literal_dbname: bool,
    dotcnt: *mut c_int,
) {
    unimplemented!()
}
