//! Translated from PostgreSQL src/include/storage/buffile.h
//!
//! Management of large buffered temporary files. In-memory/temp-file module
//! (opaque type; details live in buffile.c).
#![allow(clippy::boxed_local, reason = "TODO(stub): drop when implemented; hollow stubs mirror PG signatures 1:1; real impl consumes params")]

use crate::storage::fileset::FileSet;

/// BufFile is an opaque type whose details are not known outside buffile.c.
pub struct BufFile;

pub fn BufFileCreateTemp(_inter_xact: bool) -> Box<BufFile> {
    unimplemented!()
}
pub fn BufFileClose(_file: Box<BufFile>) {
    unimplemented!()
}
pub fn BufFileRead(_file: &mut BufFile, _ptr: &mut [u8]) -> usize {
    unimplemented!()
}
pub fn BufFileReadExact(_file: &mut BufFile, _ptr: &mut [u8]) {
    unimplemented!()
}
pub fn BufFileReadMaybeEOF(_file: &mut BufFile, _ptr: &mut [u8], _eof_ok: bool) -> usize {
    unimplemented!()
}
pub fn BufFileWrite(_file: &mut BufFile, _ptr: &[u8]) {
    unimplemented!()
}
pub fn BufFileSeek(_file: &mut BufFile, _fileno: i32, _offset: i64, _whence: i32) -> i32 {
    unimplemented!()
}
/// out-params (fileno, offset) -> tuple.
pub fn BufFileTell(_file: &mut BufFile) -> (i32, i64) {
    unimplemented!()
}
pub fn BufFileSeekBlock(_file: &mut BufFile, _blknum: i64) -> i32 {
    unimplemented!()
}
pub fn BufFileSize(_file: &mut BufFile) -> i64 {
    unimplemented!()
}
pub fn BufFileAppend(_target: &mut BufFile, _source: &mut BufFile) -> i64 {
    unimplemented!()
}

pub fn BufFileCreateFileSet(_fileset: &mut FileSet, _name: &str) -> Box<BufFile> {
    unimplemented!()
}
pub fn BufFileExportFileSet(_file: &mut BufFile) {
    unimplemented!()
}
/// missing_ok -> Option (None when absent and missing_ok).
pub fn BufFileOpenFileSet(
    _fileset: &mut FileSet,
    _name: &str,
    _mode: i32,
    _missing_ok: bool,
) -> Option<Box<BufFile>> {
    unimplemented!()
}
pub fn BufFileDeleteFileSet(_fileset: &mut FileSet, _name: &str, _missing_ok: bool) {
    unimplemented!()
}
pub fn BufFileTruncateFileSet(_file: &mut BufFile, _fileno: i32, _offset: i64) {
    unimplemented!()
}
