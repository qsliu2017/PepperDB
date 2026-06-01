//! common/parse_manifest.h - Parse a backup manifest in JSON format.

use crate::access::transam::xlogdefs::{TimeLineID, XLogRecPtr};
use crate::common::checksum_helper::pg_checksum_type;
use crate::c::{uint8, uint64, Size};
use std::ffi::{c_char, c_int, c_void};

// struct JsonManifestParseContext; (forward decl, full def below)
// typedef struct JsonManifestParseContext JsonManifestParseContext;

// typedef struct JsonManifestParseIncrementalState JsonManifestParseIncrementalState;
// Opaque struct - defined in the .c file. Use an empty repr(C) struct.
#[repr(C)]
pub struct JsonManifestParseIncrementalState {
    _private: [u8; 0],
}

// typedef void (*json_manifest_version_callback)(JsonManifestParseContext *, int manifest_version);
pub type json_manifest_version_callback =
    Option<unsafe extern "C" fn(*mut JsonManifestParseContext, manifest_version: c_int)>;

// typedef void (*json_manifest_system_identifier_callback)(JsonManifestParseContext *, uint64 manifest_system_identifier);
pub type json_manifest_system_identifier_callback =
    Option<unsafe extern "C" fn(*mut JsonManifestParseContext, manifest_system_identifier: uint64)>;

// typedef void (*json_manifest_per_file_callback)(JsonManifestParseContext *, const char *pathname,
//     uint64 size, pg_checksum_type checksum_type, int checksum_length, uint8 *checksum_payload);
pub type json_manifest_per_file_callback = Option<
    unsafe extern "C" fn(
        *mut JsonManifestParseContext,
        pathname: *const c_char,
        size: uint64,
        checksum_type: pg_checksum_type,
        checksum_length: c_int,
        checksum_payload: *mut uint8,
    ),
>;

// typedef void (*json_manifest_per_wal_range_callback)(JsonManifestParseContext *, TimeLineID tli,
//     XLogRecPtr start_lsn, XLogRecPtr end_lsn);
pub type json_manifest_per_wal_range_callback = Option<
    unsafe extern "C" fn(
        *mut JsonManifestParseContext,
        tli: TimeLineID,
        start_lsn: XLogRecPtr,
        end_lsn: XLogRecPtr,
    ),
>;

// typedef void (*json_manifest_error_callback)(JsonManifestParseContext *, const char *fmt, ...)
//     pg_attribute_printf(2, 3);
pub type json_manifest_error_callback =
    Option<unsafe extern "C" fn(*mut JsonManifestParseContext, fmt: *const c_char, ...)>;

// struct JsonManifestParseContext { ... };
#[repr(C)]
pub struct JsonManifestParseContext {
    pub private_data: *mut c_void,
    pub version_cb: json_manifest_version_callback,
    pub system_identifier_cb: json_manifest_system_identifier_callback,
    pub per_file_cb: json_manifest_per_file_callback,
    pub per_wal_range_cb: json_manifest_per_wal_range_callback,
    pub error_cb: json_manifest_error_callback,
}

// extern void json_parse_manifest(JsonManifestParseContext *context, const char *buffer, size_t size);
pub unsafe fn json_parse_manifest(
    context: *mut JsonManifestParseContext,
    buffer: *const c_char,
    size: Size,
) {
    unimplemented!()
}

// extern JsonManifestParseIncrementalState *json_parse_manifest_incremental_init(JsonManifestParseContext *context);
pub unsafe fn json_parse_manifest_incremental_init(
    context: *mut JsonManifestParseContext,
) -> *mut JsonManifestParseIncrementalState {
    unimplemented!()
}

// extern void json_parse_manifest_incremental_chunk(JsonManifestParseIncrementalState *incstate,
//     const char *chunk, size_t size, bool is_last);
pub unsafe fn json_parse_manifest_incremental_chunk(
    incstate: *mut JsonManifestParseIncrementalState,
    chunk: *const c_char,
    size: Size,
    is_last: bool,
) {
    unimplemented!()
}

// extern void json_parse_manifest_incremental_shutdown(JsonManifestParseIncrementalState *incstate);
pub unsafe fn json_parse_manifest_incremental_shutdown(
    incstate: *mut JsonManifestParseIncrementalState,
) {
    unimplemented!()
}
